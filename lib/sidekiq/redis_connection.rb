require 'connection_pool'

module Sidekiq
  class RedisConnection
    def self.create(url = nil, namespace = nil, pool = true)
      @ns ||= namespace
      @url ||= url
      if pool
        ConnectionPool.new { connect }
      else
        connect
      end
    end

    def self.connect
      r = Redis.connect(:url => @url)
      if @ns
        Redis::Namespace.new(ns, r)
      else
        r
      end
    end

    def self.namespace
      @namespace || ''
    end

    def self.url
      ENV['REDISTOGO_URL'] || @url
    end

    def self.namespace=(namespace)
      @namespace = namespace
    end
  end
end
