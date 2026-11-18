using System;
using System.Collections.Generic;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json.Serialization;

namespace Worker.Redis.SiloService.Services;

public sealed class AwsConsumerRevisionService
    : IConsumerRevisionService
{
    private readonly HttpClient _httpClient;

    public AwsConsumerRevisionService(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    public async Task<int> GetRevisionAsync(
        CancellationToken cancellationToken = default)
    {
        var metadataUri = Environment.GetEnvironmentVariable(
            "ECS_CONTAINER_METADATA_URI_V4");

        if (string.IsNullOrWhiteSpace(metadataUri))
        {
            throw new InvalidOperationException(
                "ECS_CONTAINER_METADATA_URI_V4 não encontrada.");
        }

        var metadata = await _httpClient.GetFromJsonAsync<EcsTaskMetadata>(
            $"{metadataUri}/task",
            cancellationToken);

        if (metadata is null || metadata.Revision <= 0)
        {
            throw new InvalidOperationException(
                "Não foi possível obter a revisão da Task Definition.");
        }

        return metadata.Revision;
    }

    private sealed class EcsTaskMetadata
    {
        [JsonPropertyName("Revision")]
        public int Revision { get; init; }
    }
}
