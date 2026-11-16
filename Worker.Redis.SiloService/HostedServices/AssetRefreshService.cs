using System.Text.Json;
using Worker.Redis.SiloService.Services;

namespace Worker.Redis.SiloService.HostedServices;

public sealed class AssetRefreshService(    
    IAssetCache assetCache,
    ILogger<AssetRefreshService> logger)
    : BackgroundService
{
    private readonly string _filePath = @"C:\Users\Jairon\Desktop\listaAtivos.json";

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var assets = await GetAssetsAsync();
                var snapshotBefore = assetCache.GetSnapshot();

                assetCache.UpdateAssets(assets);

                var snapshot = assetCache.GetSnapshot();

                if (snapshot.Version != snapshotBefore.Version)
                {
                    logger.LogInformation(
                        "AssetCache atualizado. Versão: {Version}, Ativos: {Count}",
                        snapshot.Version,
                        snapshot.Assets.Count());
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Erro ao atualizar AssetCache.");
            }

            await Task.Delay(
                TimeSpan.FromMinutes(2),
                stoppingToken);
        }
    }

    public async Task<IReadOnlyList<string>> GetAssetsAsync()
    {
        if (!File.Exists(_filePath))
        {
            throw new FileNotFoundException(
                $"Arquivo de ativos não encontrado: {_filePath}",
                _filePath);
        }

        await using var stream = File.OpenRead(_filePath);

        var assets = await JsonSerializer.DeserializeAsync<List<string>>(
            stream,
            new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });

        if (assets is null)
            return [];

        return assets
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim().ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }
}
