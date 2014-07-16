require "logstash/filters/base"
require "logstash/namespace"

class LogStash::Filters::MetricsParser < LogStash::Filters::Base

	#configure this filter from your logstash config
	#filter
	#{
	#	metricsParser { ... }
	#}
	config_name "metricsParser"

	#new plugins should start life at milestone 1
	milestone 1

	#by which field to split
	config :field, :validate => :string, :required => false


	public
	def register
		@previous = Hash.new{|hash, key| hash[key] = Hash.new}
		@pending = []
	end #def register

	public
	def filter(event)
		#return nothing unless there's an actual filter event
		return unless filter?(event)

		event.to_hash.each do |key, value|
			
			#parse key to three parts: priority.instance.period
			if ( result = /([^.]*)\.([^.]*)\.([^.]*)/.match(key) )
				
				evPriority=result[1]
				evInstance=result[2]
				evPeriod=result[3]

				#if field is specified, create event only for this field
				next if (@field and @field != evPeriod) 
				
				newEvent = LogStash::Event.new
				newEvent["priority"]=evPriority
				newEvent["instance"]=evInstance
				newEvent["period"]=evPeriod
				newEvent["value"]=value

				#if field was specified, count diff
				if (@field)
					#count diff
					curVal = value.to_i
					newEvent["diff"] = curVal;
					#if we already have previous value, substract it
					if ( @previous[evPriority][evInstance] )   
						newEvent["diff"] -= @previous[evPriority][evInstance]
					end
					@previous[evPriority][evInstance] = curVal;
				end

				#prepare event for sending
				newEvent.tag "parsedMetric"
				@pending << newEvent

			end

		end
		event.cancel
		
		#filter_matched should go in the last line of our successful code
		filter_matched(event)
	end

	public
	def flush
		#TODO check if need to rewrite these
		toSend = []
	
		if @pending
			@pending.each do |msg|
				toSend << msg 
			end
		end

		@pending = []
		return toSend
	end

end 
