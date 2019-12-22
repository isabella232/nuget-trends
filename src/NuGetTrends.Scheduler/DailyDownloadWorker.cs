using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MessagePack;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using NuGet.Protocol.Core.Types;
using NuGetTrends.Data;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace NuGetTrends.Scheduler
{
    // ReSharper disable once ClassNeverInstantiated.Global - DI
    public class DailyDownloadWorker : IHostedService
    {
        private readonly DailyDownloadWorkerOptions _options;
        private readonly IConnectionFactory _connectionFactory;
        private readonly IDailyDownloadService _dailyDownloadService;
        private readonly ILogger<DailyDownloadWorker> _logger;
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private readonly ConcurrentBag<(IModel,IConnection)> _connections = new ConcurrentBag<(IModel, IConnection)>();

        private readonly List<Task> _workers;

        public DailyDownloadWorker(
            IOptions<DailyDownloadWorkerOptions> options,
            IConnectionFactory connectionFactory,
            IDailyDownloadService dailyDownloadService,
            ILogger<DailyDownloadWorker> logger)
        {
            _options = options.Value;
            _connectionFactory = connectionFactory;
            _dailyDownloadService = dailyDownloadService;
            _logger = logger;
            _workers = new List<Task>(_options.WorkerCount);
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogDebug("Starting the worker.");

            for (var i = 0; i < _options.WorkerCount; i++)
            {
                _workers.Add(Task.Run(Work, _cancellationTokenSource.Token));

                void Work()
                {
                    try
                    {
                        var connection = _connectionFactory.CreateConnection();
                        var channel = connection.CreateModel();
                        _connections.Add((channel, connection));

                        const string queueName = "daily-download";
                        var queueDeclareOk = channel.QueueDeclare(queue: queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);

                        _logger.LogDebug("Queue creation OK with {QueueName}, {ConsumerCount}, {MessageCount}", queueDeclareOk.QueueName, queueDeclareOk.ConsumerCount, queueDeclareOk.MessageCount);

                        var consumer = new AsyncEventingBasicConsumer(channel);

                        consumer.Received += OnConsumerOnReceived;

                        channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);

                        var defaultConsumer = new EventingBasicConsumer(channel);

                        defaultConsumer.Received += (s, e) => { _logger.LogWarning("DefaultConsumer fired: {message}", Convert.ToBase64String(e.Body)); };

                        channel.DefaultConsumer = defaultConsumer;
                    }
                    catch (Exception e)
                    {
                        _logger.LogCritical(e, "Worker #{worker} just died.", i);
                        throw;
                    }
                }

            }
            return Task.CompletedTask;
        }

        private async Task OnConsumerOnReceived(object sender, BasicDeliverEventArgs ea)
        {
            List<string>? packageIds = null;
            try
            {
                var body = ea.Body;
                _logger.LogDebug("Received message with body size: {size}", body.Length);

                packageIds = MessagePackSerializer.Deserialize<List<string>>(body);

                if (packageIds == null)
                {
                    throw new InvalidOperationException($"Deserializing {body} resulted in a null reference.");
                }

                await _dailyDownloadService.UpdateDownloadCount(packageIds, _cancellationTokenSource.Token);

                var consumer = (AsyncEventingBasicConsumer)sender;
                consumer.Model.BasicAck(ea.DeliveryTag, false);
            }
            catch (Exception e)
            {
                if (packageIds != null)
                {
                    for (var i = 0; i < packageIds.Count; i++)
                    {
                        e.Data.Add("Package:#" + i.ToString("D2"), packageIds[i]);
                    }
                }
                _logger.LogCritical(e, "Failed to process batch.");
                throw;
            }
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogDebug("Stopping the worker.");

            try
            {
                _cancellationTokenSource.Cancel();

                if (_workers is { } workers)
                {
                    await Task.WhenAll(workers);
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Failed canceling the worker.");
            }
            finally
            {
                foreach (var (channel, connection) in _connections)
                {
                    // "Disposing channel and connection objects is not enough, they must be explicitly closed with the API methods..."
                    // https://www.rabbitmq.com/dotnet-api-guide.html
                    // - Why?
                    channel?.Close();
                    connection?.Close();
                }
            }
        }
    }
}
