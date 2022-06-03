package tp2.replica.msgs;

public record UnshareFileRep(String op, String filename, String userId, String userIdShare, String password) {
}
