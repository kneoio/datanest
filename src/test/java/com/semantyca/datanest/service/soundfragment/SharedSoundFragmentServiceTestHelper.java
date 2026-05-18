package com.semantyca.datanest.service.soundfragment;

import java.util.List;
import java.util.UUID;

class SharedSoundFragmentServiceTestHelper {

    private final SharedSoundFragmentService service =
            new SharedSoundFragmentService(null, null, null, null);

    boolean genresMatch(List<UUID> fragmentGenres, List<UUID> brandGenres) {
        return service.genresMatch(fragmentGenres, brandGenres);
    }
}
