require 'active_job/base'
require 'active_support/core_ext/numeric/time'
require 'activejob-google_cloud_pubsub/pubsub_extension'
require 'concurrent'
require 'google/cloud/pubsub'
require 'json'
require 'logger'

module ActiveJob
  module GoogleCloudPubsub
    class Worker
      MAX_DEADLINE = 10.minutes

      using PubsubExtension

      def initialize(queue: 'default', min_threads: 0, max_threads: Concurrent.processor_count, pubsub: Google::Cloud::Pubsub.new, logger: Logger.new($stdout))
        @queue_name = queue
        @min_threads = min_threads
        @max_threads = max_threads
        @pubsub = pubsub
        @logger = logger
      end

      def run
        @logger&.info "Google Pub/Sub Worker running - min_threads: #{@min_threads.to_i}, max_threads: #{@max_threads.to_i}"

        subscriber = @pubsub.subscription_for(@queue_name).listen(streams: 1, threads: { callback: 1 }) do |message|
          @logger&.info "Message(#{message.message_id}) was received."
          if message.time_to_process?
            process message
          else
            @logger&.info "Message(#{message.message_id}) is scheduled for later, skipping."
          end
        end

        subscriber.on_error do |error|
          @logger&.error(error)
        end

        subscriber.start

        while true
          sleep 5
          @logger&.info "Google Pub/Sub Worker is alive"
        end
      end

      def ensure_subscription
        @pubsub.subscription_for @queue_name

        nil
      end

      private

      def process(message)
        timer_opts = {
          execution_interval: MAX_DEADLINE - 10.seconds,
          timeout_interval: 5.seconds,
          run_now: true
        }

        delay_timer = Concurrent::TimerTask.execute(timer_opts) {
          message.modify_ack_deadline! MAX_DEADLINE.to_i
        }

        begin
          succeeded = false
          failed = false

          ActiveJob::Base.execute JSON.parse(message.data)

          succeeded = true
        rescue Exception => e
          failed = true

          @logger&.error e
          raise e
        ensure
          delay_timer.shutdown

          if succeeded || failed
            @logger&.info("Message(#{message.message_id}) processing succeeded") if succeeded
            @logger&.warn("Message(#{message.message_id}) processing failed") if failed
            message.acknowledge!

            @logger&.info "Message(#{message.message_id}) was acknowledged."
          else
            @logger&.warn "terminated from outside"
            message.modify_ack_deadline! 0
          end
        end
      end
    end
  end
end
