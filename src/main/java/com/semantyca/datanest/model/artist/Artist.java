package com.semantyca.datanest.model.artist;

import com.semantyca.core.model.SecureDataEntity;
import com.semantyca.core.model.UserData;
import com.semantyca.core.model.cnst.LanguageCode;
import lombok.Getter;
import lombok.Setter;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Setter
@Getter
@Deprecated
public class Artist extends SecureDataEntity<UUID> {
    private EnumMap<LanguageCode, String> localizedName;
    private String country;
    private List<String> styles;
    private String description;
    private Map<String, String> links;
    private UserData userData;

    public Artist() {
        this.localizedName = new EnumMap<>(LanguageCode.class);
    }
}
