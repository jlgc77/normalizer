package io.wizzie.normalizer.funcs.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.base.utils.ConversionUtils;
import io.wizzie.normalizer.funcs.MapperStoreFunction;

public class MeanStoreMapper extends MapperStoreFunction {
	List<String> counterFields;
	KeyValueStore<String, Map<String, Double>> storeCounter;
	KeyValueStore<String, Map<String, Double>> storeMeans;
	Boolean sendIfZero;
	String timestampField;
	Integer iteration;

	@SuppressWarnings("unchecked")
	@Override
	public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
		counterFields = (List<String>) properties.get("counters");
		storeCounter = getStore("counter-store");
		storeMeans = getStore("means-store");
		timestampField = String.valueOf(properties.get("timestamp"));

		if (sendIfZero == null)
			sendIfZero = true;

		timestampField = timestampField != null ? timestampField : "timestamp";
		iteration = 0;
	}

	@Override
	public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {

		Map<String, Double> means = storeMeans.get(key);
		if (means == null) {
			means = new HashMap<>();
		}
		String definedKey = "";
		KeyValue<String, Map<String, Object>> returnValue;
		iteration++;

		if (value != null) {
			definedKey = key;

			Map<String, Double> newCounters = new HashMap<>();
			Map<String, Double> newTimestamp = new HashMap<>();

			for (String counterField : counterFields) {
				Double counter = ConversionUtils.toDouble(value.remove(counterField));
				if (counter != null) {
					newCounters.put(counterField, counter);
					if (means.get(counterField) == null) {
						means.put(counterField, counter);
						storeMeans.put(key, means);
					}
				}
			}

			Double timestampValue = ConversionUtils.toDouble(value.get(timestampField));

			if (timestampValue != null) {
				newTimestamp.put(timestampField, timestampValue);
			} else {
				timestampValue = (double) (System.currentTimeMillis() / 1000);
				value.put(timestampField, timestampValue);
				newTimestamp.put(timestampField, timestampValue);
			}

			Map<String, Double> counters = storeCounter.get(definedKey);

			if (counters != null) {

				for (Map.Entry<String, Double> counter : newCounters.entrySet()) {
					Long lastValue = ConversionUtils.toLong(counters.get(counter.getKey()));
					if (lastValue != null) {
						// mean += (value - oldMean) / n
						Double oldMean = means.get(counter.getKey());
						Double newValue = counter.getValue();

						Double newMean = oldMean + ((newValue - oldMean) / iteration);

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
				for (Map.Entry<String, Double> counter : newCounters.entrySet()) {
					value.put(counter.getKey(), counter.getValue());
				}
				returnValue = new KeyValue<>(key, value);
				counters = newCounters;
			}

			counters.putAll(newTimestamp);
			storeCounter.put(definedKey, counters);

			return returnValue;
		} else {
			return new KeyValue<>(key, value);
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
		builder.append(" {").append("counters: ").append(counterFields).append(", ").append("sendIfZero: ").append(", ")
				.append("stores: ").append(storeCounter.name()).append(", ").append("timestamp: ")
				.append(timestampField).append(", ").append("keys: ").append("firstTimeView: ").append("} ");

		return builder.toString();
	}

	@Override
	public KeyValue<String, Map<String, Object>> window(long timestamp) {
		// Nothing to do.
		return null;
	}
}
