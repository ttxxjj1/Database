package db.consensus;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftState {
    public enum State {
        FOLLOWER, CANDIDATE, LEADER
    }
    
    private volatile State state;
    private volatile long electionTimeoutStart;
    private volatile long electionTimeoutDuration;
    private final AtomicInteger voteCount;
    
    private static final long MIN_ELECTION_TIMEOUT = 150;
    private static final long MAX_ELECTION_TIMEOUT = 300;
    
    public RaftState() {
        this.state = State.FOLLOWER;
        this.voteCount = new AtomicInteger(0);
        resetElectionTimeout();
    }
    
    public void becomeFollower() {
        state = State.FOLLOWER;
        voteCount.set(0);
        resetElectionTimeout();
    }
    
    public void becomeCandidate() {
        state = State.CANDIDATE;
        voteCount.set(0);
        resetElectionTimeout();
    }
    
    public void becomeLeader() {
        state = State.LEADER;
        voteCount.set(0);
    }
    
    public boolean isFollower() {
        return state == State.FOLLOWER;
    }
    
    public boolean isCandidate() {
        return state == State.CANDIDATE;
    }
    
    public boolean isLeader() {
        return state == State.LEADER;
    }
    
    public void resetElectionTimeout() {
        electionTimeoutStart = System.currentTimeMillis();
        electionTimeoutDuration = ThreadLocalRandom.current().nextLong(
            MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT + 1);
    }
    
    public boolean hasElectionTimeout() {
        return System.currentTimeMillis() - electionTimeoutStart > electionTimeoutDuration;
    }
    
    public int incrementVoteCount() {
        return voteCount.incrementAndGet();
    }
    
    public int getVoteCount() {
        return voteCount.get();
    }
    
    public State getState() {
        return state;
    }
}
