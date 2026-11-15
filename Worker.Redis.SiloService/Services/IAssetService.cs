namespace Worker.Redis.SiloService.Services;

internal interface IAssetService
{
    Task<IReadOnlyList<string>> GetActiveAssetsAsync();
}
