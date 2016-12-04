
exception SystemException {
  1: optional string message
}

enum Status {
  FAILED = 0;
  SUCCESSFUL = 1;
}

struct StatusReport {
  1: required Status status;
  2: optional string message;
}

struct RFile {
  1: required i32 tid;
  2: required string filename;
  3: required string content;
}

service Participant {

	StatusReport writeToFile(1: RFile rFile)
		throws (1: SystemException systemException),

	RFile readFromFile(1: string filename)
		throws (1: SystemException systemException),

	string vote(1: i32 tid)
		throws (1: SystemException systemException),

	bool commit(1: i32 tid)
		throws (1: SystemException systemException),

	bool abort(1: i32 tid)
		throws (1: SystemException systemException),
		
	bool releaseLock(1: string filename)
		throws (1: SystemException systemException),
}
