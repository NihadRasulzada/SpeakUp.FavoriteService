using SpeakUp.Common;
using SpeakUp.Common.Events.Entry;
using SpeakUp.Common.Events.EntryComment;
using SpeakUp.Common.Infratructure;

namespace SpeakUp.FavoriteService;

using System.Text;
using System.Text.Json;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration configuration;
    
    public Worker(ILogger<Worker> logger, IConfiguration configuration)
    {
        _logger = logger;
        this.configuration = configuration;
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var connStr = configuration.GetConnectionString("SqlServer");
        var favService = new Services.FavoriteService(connStr);

        var createEntryFavConsumer = await (await (await QueueFactory.CreateBasicConsumerAsync())
                .EnsureExchangeAsync(SpeakUpConstants.FavExchangeName))
            .EnsureQueueAsync(SpeakUpConstants.CreateEntryFavQueueName, SpeakUpConstants.FavExchangeName);

        createEntryFavConsumer.ReceivedAsync += async (model, ea) =>
        {
            try
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var fav = JsonSerializer.Deserialize<CreateEntryFavEvent>(message);

                await favService.CreateEntryFav(fav);
                _logger.LogInformation($"Received EntryId {fav.EntryId}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing CreateEntryFavEvent");
            }
        };

        await createEntryFavConsumer.StartConsumingAsync(SpeakUpConstants.CreateEntryFavQueueName);

        var deleteEntryFavConsumer = await (await (await QueueFactory.CreateBasicConsumerAsync())
                .EnsureExchangeAsync(SpeakUpConstants.FavExchangeName))
            .EnsureQueueAsync(SpeakUpConstants.DeleteEntryFavQueueName, SpeakUpConstants.FavExchangeName);

        deleteEntryFavConsumer.ReceivedAsync += async (model, ea) =>
        {
            try
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var fav = JsonSerializer.Deserialize<DeleteEntryFavEvent>(message);

                await favService.DeleteEntryFav(fav);
                _logger.LogInformation($"Deleted Received EntryId {fav.EntryId}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing DeleteEntryFavEvent");
            }
        };

        await deleteEntryFavConsumer.StartConsumingAsync(SpeakUpConstants.DeleteEntryFavQueueName);

        var createCommentFavConsumer = await (await (await QueueFactory.CreateBasicConsumerAsync())
                .EnsureExchangeAsync(SpeakUpConstants.FavExchangeName))
            .EnsureQueueAsync(SpeakUpConstants.CreateEntryCommentFavQueueName, SpeakUpConstants.FavExchangeName);

        createCommentFavConsumer.ReceivedAsync += async (model, ea) =>
        {
            try
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var fav = JsonSerializer.Deserialize<CreateEntryCommentFavEvent>(message);

                await favService.CreateEntryCommentFav(fav);
                _logger.LogInformation($"Create EntryComment Received EntryCommentId {fav.EntryCommentId}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing CreateEntryCommentFavEvent");
            }
        };

        await createCommentFavConsumer.StartConsumingAsync(SpeakUpConstants.CreateEntryCommentFavQueueName);

        var deleteCommentFavConsumer = await (await (await QueueFactory.CreateBasicConsumerAsync())
                .EnsureExchangeAsync(SpeakUpConstants.FavExchangeName))
            .EnsureQueueAsync(SpeakUpConstants.DeleteEntryCommentFavQueueName, SpeakUpConstants.FavExchangeName);

        deleteCommentFavConsumer.ReceivedAsync += async (model, ea) =>
        {
            try
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var fav = JsonSerializer.Deserialize<DeleteEntryCommentFavEvent>(message);

                await favService.DeleteEntryCommentFav(fav);
                _logger.LogInformation($"Deleted Received EntryCommentId {fav.EntryCommentId}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing DeleteEntryCommentFavEvent");
            }
        };

        await deleteCommentFavConsumer.StartConsumingAsync(SpeakUpConstants.DeleteEntryCommentFavQueueName);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }
}