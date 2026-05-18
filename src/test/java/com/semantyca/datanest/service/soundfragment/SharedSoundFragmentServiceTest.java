package com.semantyca.datanest.service.soundfragment;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedSoundFragmentServiceTest {

    private final SharedSoundFragmentServiceTestHelper helper = new SharedSoundFragmentServiceTestHelper();

    private static final UUID GENRE_A = UUID.randomUUID();
    private static final UUID GENRE_B = UUID.randomUUID();
    private static final UUID GENRE_C = UUID.randomUUID();

    @Test
    void brandWithNoGenresAcceptsAnySong() {
        assertTrue(helper.genresMatch(List.of(GENRE_A), List.of()));
        assertTrue(helper.genresMatch(List.of(GENRE_A), null));
    }

    @Test
    void songWithNoGenresIsAcceptedByAnyBrand() {
        assertTrue(helper.genresMatch(List.of(), List.of(GENRE_A)));
        assertTrue(helper.genresMatch(null, List.of(GENRE_A)));
    }

    @Test
    void matchWhenAtLeastOneGenreOverlaps() {
        assertTrue(helper.genresMatch(List.of(GENRE_B, GENRE_C), List.of(GENRE_A, GENRE_B)));
    }

    @Test
    void noMatchWhenNoGenresOverlap() {
        assertFalse(helper.genresMatch(List.of(GENRE_C), List.of(GENRE_A, GENRE_B)));
    }

    @Test
    void noMatchWhenBothListsNonEmptyAndDisjoint() {
        assertFalse(helper.genresMatch(List.of(GENRE_A), List.of(GENRE_B, GENRE_C)));
    }
}
