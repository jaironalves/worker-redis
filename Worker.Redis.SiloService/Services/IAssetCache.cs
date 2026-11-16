namespace Worker.Redis.SiloService.Services;

public interface IAssetCache
{
    AssetCacheSnapshot GetSnapshot();

    void UpdateAssets(IReadOnlyList<string> assets);
}
