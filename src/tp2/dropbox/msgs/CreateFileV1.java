package tp2.dropbox.msgs;

public record CreateFileV1(String path, String mode, boolean autorename, boolean mute, boolean strict_conflict) {
}
