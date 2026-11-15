using System.Text.Json;

namespace Worker.Redis.SiloService.Services;

internal sealed class JsonAssetService : IAssetService
{
    private readonly string _filePath = @"C:\Users\Jairon\Desktop\listaAtivos.json";

    public async Task<IReadOnlyList<string>> GetActiveAssetsAsync()
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
