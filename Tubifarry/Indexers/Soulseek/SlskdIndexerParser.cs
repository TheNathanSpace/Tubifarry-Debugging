using FuzzySharp;
using NLog;
using NzbDrone.Common.Http;
using NzbDrone.Common.Instrumentation;
using NzbDrone.Core.Download;
using NzbDrone.Core.Indexers;
using NzbDrone.Core.Lifecycle;
using NzbDrone.Core.Messaging.Events;
using NzbDrone.Core.Parser.Model;
using System.Text.Json;
using Tubifarry.Core.Model;
using Tubifarry.Core.Utilities;

namespace Tubifarry.Indexers.Soulseek
{
    public class SlskdIndexerParser : IParseIndexerResponse, IHandle<AlbumGrabbedEvent>, IHandle<ApplicationShutdownRequested>
    {
        private readonly SlskdIndexer _indexer;
        private readonly Logger _logger;
        private readonly Lazy<IIndexerFactory> _indexerFactory;
        private readonly IHttpClient _httpClient;
        private readonly ISlskdItemsParser _itemsParser;

        private static readonly Dictionary<int, string> _interactiveResults = [];
        private static readonly Dictionary<string, (HashSet<string> IgnoredUsers, long LastFileSize)> _ignoreListCache = new();

        private SlskdSettings Settings => _indexer.Settings;

        public SlskdIndexerParser(SlskdIndexer indexer, Lazy<IIndexerFactory> indexerFactory, IHttpClient httpClient, ISlskdItemsParser itemsParser)
        {
            _indexer = indexer;
            _indexerFactory = indexerFactory;
            _logger = NzbDroneLogger.GetLogger(this);
            _httpClient = httpClient;
            _itemsParser = itemsParser;
        }

        public IList<ReleaseInfo> ParseResponse(IndexerResponse indexerResponse)
        {
            List<AlbumData> albumDatas = [];
            try
            {
                SlskdSearchResponse? searchResponse = JsonSerializer.Deserialize<SlskdSearchResponse>(indexerResponse.Content, IndexerParserHelper.StandardJsonOptions);

                if (searchResponse == null)
                {
                    _logger.Error("Failed to deserialize slskd search response.");
                    return [];
                }

                SlskdSearchData searchTextData = SlskdSearchData.FromJson(indexerResponse.HttpRequest.ContentSummary);
                HashSet<string>? ignoredUsers = GetIgnoredUsers(Settings.IgnoreListPath);

                _logger.Debug($"Parsing search results: DirectoryExpansion={searchTextData.ExpandDirectory}, FilterAudioOnly={Settings.FilterUnfittingAlbums}, MinFiles={searchTextData.MinimumFiles}, MaxFiles={searchTextData.MaximumFiles}");

                foreach (SlskdFolderData response in searchResponse.Responses)
                {
                    if (ignoredUsers?.Contains(response.Username) == true)
                    {
                        _logger.Debug($"Ignoring response from {response.Username}: User is in ignore list.");
                        continue;
                    }

                    IEnumerable<SlskdFileData> filteredFiles = SlskdFileData.GetFilteredFiles(response.Files, Settings.OnlyAudioFiles, Settings.IncludeFileExtensions);

                    foreach (IGrouping<string, SlskdFileData> directoryGroup in filteredFiles.GroupBy(f => SlskdTextProcessor.GetDirectoryFromFilename(f.Filename)))
                    {
                        if (string.IsNullOrEmpty(directoryGroup.Key))
                            continue;

                        SlskdFolderData folderData = _itemsParser.ParseFolderName(directoryGroup.Key) with
                        {
                            Username = response.Username,
                            HasFreeUploadSlot = response.HasFreeUploadSlot,
                            UploadSpeed = response.UploadSpeed,
                            LockedFileCount = response.LockedFileCount,
                            LockedFiles = response.LockedFiles,
                            QueueLength = response.QueueLength,
                            Token = response.Token,
                            FileCount = response.FileCount
                        };

                        _logger.Debug($"Parsed folder: {directoryGroup.Key} -> Artist: '{folderData.Artist}', Album: '{folderData.Album}', Year: '{folderData.Year}'");

                        IGrouping<string, SlskdFileData> finalGroup = directoryGroup;
                        if (searchTextData.ExpandDirectory)
                        {
                            IGrouping<string, SlskdFileData>? expandedGroup = TryExpandDirectory(searchTextData, directoryGroup, folderData);
                            if (expandedGroup != null)
                                finalGroup = expandedGroup;
                        }

                        if (searchTextData.MinimumFiles > 0 || searchTextData.MaximumFiles.HasValue)
                        {
                            bool filterAudioOnly = Settings.FilterUnfittingAlbums;
                            int fileCount = filterAudioOnly
                                ? finalGroup.Count(f => AudioFormatHelper.GetAudioCodecFromExtension(f.Extension ?? Path.GetExtension(f.Filename) ?? "") != AudioFormat.Unknown)
                                : finalGroup.Count();

                            _logger.Debug($"Evaluating track count for {directoryGroup.Key}: Found {fileCount} {(filterAudioOnly ? "audio tracks" : "files")} (Min: {searchTextData.MinimumFiles}, Max: {searchTextData.MaximumFiles?.ToString() ?? "N/A"})");

                            if (fileCount < searchTextData.MinimumFiles)
                            {
                                _logger.Debug($"Filtered (too few): {directoryGroup.Key} ({fileCount}/{searchTextData.MinimumFiles} {(filterAudioOnly ? "audio tracks" : "files")})");
                                continue;
                            }

                            if (searchTextData.MaximumFiles.HasValue && fileCount > searchTextData.MaximumFiles.Value)
                            {
                                _logger.Debug($"Filtered (too many): {directoryGroup.Key} ({fileCount}/{searchTextData.MaximumFiles} {(filterAudioOnly ? "audio tracks" : "files")})");
                                continue;
                            }

                            _logger.Debug($"Accepted: {directoryGroup.Key} with {fileCount} {(filterAudioOnly ? "audio tracks" : "files")}");
                        }

                        int priority = folderData.CalculatePriority(searchTextData.MinimumFiles);
                        if (priority == 0)
                        {
                            _logger.Debug($"Filtered (low priority/quality): {directoryGroup.Key} (Score: 0). Possible reasons: >50% locked files, or missing >50% of expected tracks ({searchTextData.MinimumFiles}).");
                            continue;
                        }

                        AlbumData albumData = _itemsParser.CreateAlbumData(searchResponse.Id, finalGroup, searchTextData, folderData, Settings, searchTextData.MinimumFiles);
                        albumDatas.Add(albumData);
                    }
                }

                RemoveSearch(searchResponse.Id, albumDatas.Count != 0 && searchTextData.Interactive);
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Failed to parse Slskd search response.");
            }

            return albumDatas.OrderByDescending(x => x.Priotity).Select(a => a.ToReleaseInfo()).ToList();
        }

        private IGrouping<string, SlskdFileData>? TryExpandDirectory(SlskdSearchData searchTextData, IGrouping<string, SlskdFileData> directoryGroup, SlskdFolderData folderData)
        {
            if (string.IsNullOrEmpty(searchTextData.Artist) || string.IsNullOrEmpty(searchTextData.Album))
            {
                _logger.Debug($"Skipping directory expansion for {directoryGroup.Key}: Artist or Album missing from search query.");
                return null;
            }

            bool artistMatch = Fuzz.PartialRatio(folderData.Artist, searchTextData.Artist) > 85;
            bool albumMatch = Fuzz.PartialRatio(folderData.Album, searchTextData.Album) > 85;

            if (!artistMatch || !albumMatch)
            {
                _logger.Debug($"Skipping directory expansion for {directoryGroup.Key}: Fuzzy match failed (Artist: {artistMatch}, Album: {albumMatch})");
                return null;
            }

            SlskdFileData? originalTrack = directoryGroup.FirstOrDefault(x => AudioFormatHelper.GetAudioCodecFromExtension(x.Extension?.ToLowerInvariant() ?? Path.GetExtension(x.Filename) ?? "") != AudioFormat.Unknown);

            if (originalTrack == null)
            {
                _logger.Debug($"Skipping directory expansion for {directoryGroup.Key}: No audio tracks found in initial directory.");
                return null;
            }

            _logger.Debug($"Expanding directory for: {folderData.Username}:{directoryGroup.Key}");

            SlskdRequestGenerator? requestGenerator = _indexer.GetExtendedRequestGenerator() as SlskdRequestGenerator;
            IGrouping<string, SlskdFileData>? expandedGroup = requestGenerator?.ExpandDirectory(folderData.Username, directoryGroup.Key, originalTrack).GetAwaiter().GetResult();

            if (expandedGroup != null)
            {
                _logger.Debug($"Successfully expanded directory to {expandedGroup.Count()} files for {directoryGroup.Key}");
                return expandedGroup;
            }
            else
            {
                _logger.Debug($"Failed to expand directory for {folderData.Username}:{directoryGroup.Key}");
            }
            return null;
        }

        public void RemoveSearch(string searchId, bool delay = false)
        {
            Task.Run(async () =>
            {
                try
                {
                    if (delay)
                    {
                        _interactiveResults.TryGetValue(_indexer.Definition.Id, out string? staleId);
                        _interactiveResults[_indexer.Definition.Id] = searchId;
                        if (staleId != null)
                            searchId = staleId;
                        else return;
                    }
                    await ExecuteRemovalAsync(Settings, searchId);
                }
                catch (HttpException ex)
                {
                    _logger.Error(ex, $"Failed to remove slskd search with ID: {searchId}");
                }
            });
        }

        public void Handle(AlbumGrabbedEvent message)
        {
            if (!_interactiveResults.TryGetValue(message.Album.Release.IndexerId, out string? selectedId) || !message.Album.Release.InfoUrl.EndsWith(selectedId))
                return;
            ExecuteRemovalAsync((SlskdSettings)_indexerFactory.Value.Get(message.Album.Release.IndexerId).Settings, selectedId).GetAwaiter().GetResult();
            _interactiveResults.Remove(message.Album.Release.IndexerId);
        }

        public void Handle(ApplicationShutdownRequested message)
        {
            foreach (int indexerId in _interactiveResults.Keys.ToList())
            {
                if (_interactiveResults.TryGetValue(indexerId, out string? selectedId))
                {
                    ExecuteRemovalAsync((SlskdSettings)_indexerFactory.Value.Get(indexerId).Settings, selectedId).GetAwaiter().GetResult();
                    _interactiveResults.Remove(indexerId);
                }
            }
        }

        public static void InvalidIgnoreCache(string path) => _ignoreListCache.Remove(path);

        private async Task ExecuteRemovalAsync(SlskdSettings settings, string searchId)
        {
            try
            {
                HttpRequest request = new HttpRequestBuilder($"{settings.BaseUrl}/api/v0/searches/{searchId}")
                    .SetHeader("X-API-KEY", settings.ApiKey)
                    .Build();
                request.Method = HttpMethod.Delete;
                await _httpClient.ExecuteAsync(request);
            }
            catch (HttpException ex)
            {
                _logger.Error(ex, $"Failed to remove slskd search with ID: {searchId}");
            }
        }

        private HashSet<string>? GetIgnoredUsers(string? ignoreListPath)
        {
            if (string.IsNullOrWhiteSpace(ignoreListPath) || !File.Exists(ignoreListPath))
                return null;

            try
            {
                FileInfo fileInfo = new(ignoreListPath);
                long fileSize = fileInfo.Length;

                if (_ignoreListCache.TryGetValue(ignoreListPath, out (HashSet<string> IgnoredUsers, long LastFileSize) cached) && cached.LastFileSize == fileSize)
                {
                    _logger.Trace($"Using cached ignore list from: {ignoreListPath} with {cached.IgnoredUsers.Count} users");
                    return cached.IgnoredUsers;
                }
                HashSet<string> ignoredUsers = SlskdTextProcessor.ParseListContent(File.ReadAllText(ignoreListPath));
                _ignoreListCache[ignoreListPath] = (ignoredUsers, fileSize);
                _logger.Trace($"Loaded ignore list with {ignoredUsers.Count} users from: {ignoreListPath}");
                return ignoredUsers;
            }
            catch (Exception ex)
            {
                _logger.Warn(ex, $"Failed to load ignore list from: {ignoreListPath}");
                return null;
            }
        }
    }
}