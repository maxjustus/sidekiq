require 'helper'
require 'sidekiq'
require 'sidekiq/manager'
require 'timed_queue'

class TestManager < MiniTest::Unit::TestCase
  describe 'with redis' do
    before do
      Sidekiq::Client.redis = @redis = Redis.connect(:url => 'redis://localhost/sidekiq_test')
      @redis.flushdb
      $processed = 0
    end

    class IntegrationWorker
      include Sidekiq::Worker

      def perform(a, b)
        $processed += 1
        a + b
      end
    end

    def run_manager(job_count = 2, namespace = '')
      q = TimedQueue.new
      mgr = Sidekiq::Manager.new("redis://localhost/sidekiq_test", :queues => [:foo], :namespace => namespace)
      mgr.when_done do |_|
        q << 'done' if $processed == job_count 
      end
      mgr.start!
      result = q.timed_pop
      mgr.stop
      result
    end

    it 'processes messages' do
      Sidekiq::Client.push(:foo, 'class' => IntegrationWorker, 'args' => [1, 2])
      Sidekiq::Client.push(:foo, 'class' => IntegrationWorker, 'args' => [1, 2])

      assert_equal 'done', run_manager(2)
    end

    it 'processes message in namespace' do
      Sidekiq::Client.push('foo', {'class' => IntegrationWorker, 'args' => [1, 2]}, 'derp')

      assert_equal 'done', run_manager(1, 'derp')
    end
  end
end
