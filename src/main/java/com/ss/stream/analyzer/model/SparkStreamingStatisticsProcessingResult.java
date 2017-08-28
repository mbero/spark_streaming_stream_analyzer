package com.ss.stream.analyzer.model;

public class SparkStreamingStatisticsProcessingResult {

	private String readTag_id;
	private Double summer;
	private Double counter;
	private Double sumsqr;
	private Double delta;
	private Double bestmin;
	private Double bestmax;
	private Double mean;
	private Double m2;
	private Double var;
	
	
	public SparkStreamingStatisticsProcessingResult(String readTag_id, Double summer, Double counter, Double sumsqr,
			Double delta, Double bestmin, Double bestmax, Double mean, Double m2, Double var) {
		super();
		this.readTag_id = readTag_id;
		this.summer = summer;
		this.counter = counter;
		this.sumsqr = sumsqr;
		this.delta = delta;
		this.bestmin = bestmin;
		this.bestmax = bestmax;
		this.mean = mean;
		this.m2 = m2;
		this.var = var;
	}
	
	public String getReadTag_id() {
		return readTag_id;
	}
	public void setReadTag_id(String readTag_id) {
		this.readTag_id = readTag_id;
	}
	public Double getSummer() {
		return summer;
	}
	public void setSummer(Double summer) {
		this.summer = summer;
	}
	public Double getCounter() {
		return counter;
	}
	public void setCounter(Double counter) {
		this.counter = counter;
	}
	public Double getSumsqr() {
		return sumsqr;
	}
	public void setSumsqr(Double sumsqr) {
		this.sumsqr = sumsqr;
	}
	public Double getDelta() {
		return delta;
	}
	public void setDelta(Double delta) {
		this.delta = delta;
	}
	public Double getBestmin() {
		return bestmin;
	}
	public void setBestmin(Double bestmin) {
		this.bestmin = bestmin;
	}
	public Double getBestmax() {
		return bestmax;
	}
	public void setBestmax(Double bestmax) {
		this.bestmax = bestmax;
	}
	public Double getMean() {
		return mean;
	}
	public void setMean(Double mean) {
		this.mean = mean;
	}
	public Double getM2() {
		return m2;
	}
	public void setM2(Double m2) {
		this.m2 = m2;
	}
	public Double getVar() {
		return var;
	}
	public void setVar(Double var) {
		this.var = var;
	}

	
}
