require 'multi_json'
require 'redis'

module Sidekiq
  class Client

    def self.redis
      @redis ||= begin
        # autoconfig for Heroku
        hash = {}
        hash[:url] = ENV['REDISTOGO_URL'] if ENV['REDISTOGO_URL']
        Redis.connect(hash)
      end
    end

    def self.redis=(redis)
      @redis = redis
    end

    # Example usage:
    # Sidekiq::Client.push('my_queue', 'class' => MyWorker, 'args' => ['foo', 1, :bat => 'bar'])
    def self.push(*args)
      if args.length == 1
        item = args[0]
        queue = 'default'
        namespace = ''
      else
        queue, item, namespace = args
      end

      raise(ArgumentError, "Message must be a Hash of the form: { 'class' => SomeClass, 'args' => ['bob', 1, :foo => 'bar'] }") unless item.is_a?(Hash)
      raise(ArgumentError, "Message must include a class and set of arguments: #{item.inspect}") if !item['class'] || !item['args']

      item['class'] = item['class'].to_s if !item['class'].is_a?(String)
      redis.rpush("#{namespace}queue:#{queue}", MultiJson.encode(item))
    end

    # Please use .push if possible instead.
    #
    # Example usage:
    #
    #   Sidekiq::Client.enqueue(MyWorker, 'foo', 1, :bat => 'bar')
    #
    # Messages are enqueued to the 'default' queue.  Optionally,
    # MyWorker can define a queue class method:
    #
    #   def self.queue
    #     'my_queue'
    #   end
    #
    def self.enqueue(klass, *args)
      queue = (klass.respond_to?(:queue) && klass.queue) || 'default'
      push(queue, { 'class' => klass.name, 'args' => args })
    end
  end
end
