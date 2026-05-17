package com.semantyca.datanest.util;

import com.ibm.icu.text.Transliterator;

import java.text.Normalizer;
import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class SlugHelper {

    private static final Transliterator TO_ASCII_LATIN = Transliterator.getInstance("Any-Latin; Latin-ASCII");

    private static final Pattern WHITESPACE = Pattern.compile("\\s+", Pattern.UNICODE_CHARACTER_CLASS);
    private static final Pattern COMBINING_MARKS = Pattern.compile("\\p{M}+");
    private static final Pattern NON_ASCII_ALNUM_RUNS = Pattern.compile("[^a-z0-9]+");
    private static final Pattern MULTIPLE_DASHES = Pattern.compile("-+");

    private SlugHelper() {
    }

    public static String generateSlug(String element1, String element2) {
        if (element1 == null) {
            element1 = "";
        }
        if (element2 == null) {
            element2 = "";
        }
        return processSlug(element1 + " " + element2);
    }

    public static String generateSlugPath(String... segments) {
        if (segments == null || segments.length == 0) {
            return "";
        }

        return Arrays.stream(segments)
                .map(SlugHelper::generateSlug)
                .filter(slug -> !slug.isEmpty())
                .collect(Collectors.joining("/"));
    }

    public static String generateSlug(String input) {
        if (input == null || input.trim().isEmpty()) {
            return "";
        }

        String[] stemAndExt = splitPreservingKnownExtension(input);
        return processSlug(stemAndExt[0]) + stemAndExt[1];
    }

    private static String[] splitPreservingKnownExtension(String input) {
        int lastDotIndex = input.lastIndexOf('.');
        if (lastDotIndex > 0 && lastDotIndex < input.length() - 1) {
            String extCandidate = input.substring(lastDotIndex + 1);
            if (isKnownFileExtension(extCandidate)) {
                return new String[]{input.substring(0, lastDotIndex), input.substring(lastDotIndex)};
            }
        }
        return new String[]{input, ""};
    }

    private static boolean isKnownFileExtension(String extensionWithoutDot) {
        return switch (extensionWithoutDot.toLowerCase(Locale.ROOT)) {
            case "mp3", "wav", "ogg", "flac", "jpg", "jpeg", "png", "gif", "webp", "pdf" -> true;
            default -> false;
        };
    }

    private static String processSlug(String input) {
        if (input == null || input.trim().isEmpty()) {
            return "";
        }

        String s = WHITESPACE.matcher(input).replaceAll("-");
        s = Normalizer.normalize(s, Normalizer.Form.NFKC);
        s = TO_ASCII_LATIN.transliterate(s);
        s = s.toLowerCase(Locale.ROOT);
        s = Normalizer.normalize(s, Normalizer.Form.NFKD);
        s = COMBINING_MARKS.matcher(s).replaceAll("");
        s = NON_ASCII_ALNUM_RUNS.matcher(s).replaceAll("-");
        s = MULTIPLE_DASHES.matcher(s).replaceAll("-");
        s = s.replaceAll("^-+|-+$", "");

        return s;
    }
}
