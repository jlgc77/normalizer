package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.base.utils.ConversionUtils;
import io.wizzie.normalizer.funcs.MapperStoreFunction;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.wizzie.normalizer.base.utils.Constants.__KEY;

public class AverangeStoreMapper extends MapperStoreFunction {
	List<String> counterFields;
	KeyValueStore<String, Map<String, Long>> storeCounter;
	KeyValueStore<String, Map<String, Long>> storeMeans;
	Boolean sendIfZero;
	String timestampField;
	Integer iteration;

	@Override
	public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
		counterFields = (List<String>) properties.get("counters");
		storeCounter = getStore("counter-store");
		storeMeans = getStore("means-store");
		sendIfZero = (Boolean) properties.get("sendIfZero");
		timestampField = String.valueOf(properties.get("timestamp"));

		if (sendIfZero == null)
			sendIfZero = true;

		timestampField = timestampField != null ? timestampField : "timestamp";
		iteration = 0;
	}

	@Override
	public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {

		Map<String, Long> means = storeMeans.get(key);
		if (means == null) {
			means = new HashMap<>();
		}
		String definedKey = "";
		KeyValue<String, Map<String, Object>> returnValue;
		iteration++;

		if (value != null) {
			definedKey = key;

			Map<String, Long> newCounters = new HashMap<>();
			Map<String, Long> newTimestamp = new HashMap<>();

			for (String counterField : counterFields) {
				Long counter = ConversionUtils.toLong(value.remove(counterField));
				if (counter != null) {
					newCounters.put(counterField, counter);
					if (means.get(counterField) == null) {
						means.put(counterField, counter);
						storeMeans.put(key, means);
					}
				}
			}

			Long timestampValue = ConversionUtils.toLong(value.get(timestampField));

			if (timestampValue != null) {
				newTimestamp.put(timestampField, timestampValue);
			} else {
				timestampValue = System.currentTimeMillis() / 1000;
				value.put(timestampField, timestampValue);
				newTimestamp.put(timestampField, timestampValue);
			}

			Map<String, Long> counters = storeCounter.get(definedKey);

			if (counters != null) {

				for (Map.Entry<String, Long> counter : newCounters.entrySet()) {
					Long lastValue = ConversionUtils.toLong(counters.get(counter.getKey()));
					if (lastValue != null) {
						// mean += (value - oldMean) / n
						Long mean = means.get(counter.getKey());
						Long newValue = counter.getValue();
						Long oldValue = lastValue;

						Long newMean = mean + ((newValue - oldValue) / iteration);

						value.put(counter.getKey(), newMean);
						means.put(counter.getKey(), newMean);
					}
				}

				Long lastTimestamp = ConversionUtils.toLong(counters.get(this.timestampField));

				if (lastTimestamp != null) {
					value.put("last_timestamp", lastTimestamp);
				}

				returnValue = new KeyValue<>(key, value);
				counters.putAll(newCounters);
			} else {
				returnValue = new KeyValue<>(key, value);
				counters = newCounters;
			}

			counters.putAll(newTimestamp);
			storeCounter.put(definedKey, counters);

			return returnValue;
		} else {
			return new KeyValue<>(null, null);
		}
	}

	@Override
	public void stop() {
		if (counterFields != null)
			counterFields.clear();
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(" {").append("counters: ").append(counterFields).append(", ").append("sendIfZero: ")
				.append(sendIfZero).append(", ").append("stores: ").append(storeCounter.name()).append(", ")
				.append("timestamp: ").append(timestampField).append(", ").append("keys: ").append("firstTimeView: ")
				.append("} ");

		return builder.toString();
	}

	@Override
	public KeyValue<String, Map<String, Object>> window(long timestamp) {
		// Nothing to do.
		return null;
	}
}
