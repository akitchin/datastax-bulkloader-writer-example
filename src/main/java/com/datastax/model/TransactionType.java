package com.datastax.model;

public enum TransactionType {
	DEBIT("D"), CREDIT("C");

	private String value;

	TransactionType(String value){
		this.value = value;
	}
	
	public String getValue(){
		return this.value;
	}
}
