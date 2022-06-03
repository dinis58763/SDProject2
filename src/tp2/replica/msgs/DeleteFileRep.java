package tp2.replica.msgs;

public record DeleteFileRep(String op, String filename, String userId, String password) {
}
