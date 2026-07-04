package com.semantyca.datanest.dto.stream;

import com.semantyca.mixpla.model.ScenePrompt;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;

@Setter
@Getter
public class StreamScheduleDTO {
    private OffsetDateTime createdAt;
    private OffsetDateTime estimatedEndTime;
    private int totalScenes;
    private int totalSongs;
    private List<SceneScheduleDTO> scenes;

    @Setter
    @Getter
    public static class SceneScheduleDTO {
        private String sceneId;
        private String sceneTitle;
        private OffsetDateTime scheduledStartTime;
        private OffsetDateTime scheduledEndTime;
        private int durationSeconds;
        private double dayPercentage;
        private List<ScheduledSongDTO> songs;

        private LocalTime originalStartTime;
        private LocalTime originalEndTime;
        private ScenePlaylistRequest playlistRequest;
        private String warning;
    }

    @Setter
    @Getter
    public static class ScenePlaylistRequest {
        private String sourcing;
        private String playlistTitle;
        private String artist;
        private List<java.util.UUID> genres;
        private List<java.util.UUID> labels;
        private List<String> playlistItemTypes;
        private List<String> sourceTypes;
        private String searchTerm;
        private List<java.util.UUID> soundFragments;
        private List<ScenePrompt> contentPrompts;
    }

    @Setter
    @Getter
    public static class ScheduledSongDTO {
        private String id;
        private String songId;
        private String title;
        private String artist;
        private OffsetDateTime scheduledStartTime;
        private int estimatedDurationSeconds;
    }
}
