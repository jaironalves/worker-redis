using System.Collections.Immutable;

namespace Worker.Redis.SiloService.Services;

public sealed class AssetCache : IAssetCache
{
    private AssetCacheSnapshot _snapshot = new(
        Version: 0,
        Assets: []);

    public AssetCacheSnapshot GetSnapshot()
    {
        return Volatile.Read(ref _snapshot);
    }

    public void UpdateAssets(IReadOnlyList<string> assets)
    {
        ArgumentNullException.ThrowIfNull(assets);

        var normalized = assets
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim().ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
            .ToImmutableArray();

        var current = Volatile.Read(ref _snapshot);

        // Não atualiza se a composição dos ativos não mudou.
        if (current.Assets.SequenceEqual(
                normalized,
                StringComparer.OrdinalIgnoreCase))
        {
            return;
        }

        var newSnapshot = new AssetCacheSnapshot(
            Version: current.Version + 1,
            Assets: normalized);

        Volatile.Write(ref _snapshot, newSnapshot);
    }
}