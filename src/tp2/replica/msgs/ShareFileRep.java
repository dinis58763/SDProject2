package tp2.replica.msgs;

public record ShareFileRep(String op, String filename, String userId, String userIdShare, String password) {
}
