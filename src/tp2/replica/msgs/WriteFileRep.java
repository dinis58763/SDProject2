package tp2.replica.msgs;

public record WriteFileRep(String op, String filename, byte[] data, String userId, String password) {
}