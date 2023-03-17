from data_collection.spotify.audio_features.audio_features_collection_runner import AudioFeaturesCollectorRunner
from data_collection.spotify.radio_stations_snapshots_runner import RadioStationsSnapshotsRunner


def run() -> None:
    RadioStationsSnapshotsRunner().run()
    AudioFeaturesCollectorRunner().run()


if __name__ == '__main__':
    run()
