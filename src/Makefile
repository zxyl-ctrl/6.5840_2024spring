run_mr_worker:
	@go build -buildmode=plugin -o ./mrapps/plugin/ ./mrapps/wc.go 
	@go run ./main/mrworker.go ./mrapps/plugin/wc.so

run_mr_coo:
	@go run ./main/mrcoordinator.go ./main/pg-being_ernest.txt ./main/pg-dorian_gray.txt ./main/pg-frankenstein.txt ./main/pg-grimm.txt ./main/pg-huckleberry_finn.txt ./main/pg-metamorphosis.txt ./main/pg-sherlock_holmes.txt ./main/pg-tom_sawyer.txt

run_test_kvsrv:
	@go test ./kvsrv -count=1

run_test_raft:
	@go test ./raft -run Test.*3A -count=1
	@go test ./raft -run Test.*3B -count=1
	@go test ./raft -run Test.*3C -count=1
	@go test ./raft -run Test.*3D -count=1

run_test_raft_temp:
	#@go test ./rafttemp -run Test.*3A -count=1
	#@go test ./rafttemp -run Test.*3B -count=1
	#@go test ./rafttemp -run Test.*3C -count=1
	@go test ./rafttemp -run Test.*3D -count=1

run_test_kvraft:
	@go test ./kvraft -run Test.*4A -count=1
	@go test ./kvraft -run Test.*4B -count=1

run_test_kvrafttemp:
	@go test ./kvrafttemp -run Test.*4A -count=1
	@go test ./kvrafttemp -run Test.*4B -count=1

run_test_shardedKV:
	@go test ./shardctrler -count=1
	@go test ./shardkv -run Test.*5A -count=1
	@go test ./shardkv -run Test.*5B -count=1
	@go test ./shardkv -run TestChallenge -count=1

run_test_shardedKVtemp:
	@go test ./shardctrlertemp -count=1
	@go test ./shardkv -run Test.*5A -count=1