package com.my.twopc.participant.store.impl;

import org.apache.thrift.TException;

import com.my.twopc.custom.exception.SystemException;
import com.my.twopc.model.RFile;
import com.my.twopc.model.StatusReport;
import com.my.twopc.participant.store.Participant.Iface;

public class ParticipantImpl implements Iface {

	@Override
	public StatusReport writeToFile(RFile rFile) throws SystemException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RFile readFromFile(String filename) throws SystemException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String vote(int tid) throws SystemException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean commit(int tid) throws SystemException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean abort(int tid) throws SystemException, TException {
		// TODO Auto-generated method stub
		return false;
	}

}
