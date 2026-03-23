import json
from typing import Optional, final

import pycer
from ..abstract_streamer_websocket import AbstractStreamerWebsocket
from .models.commit import CommitWrapperEventModel


@final
class CreatePostStreamer(AbstractStreamerWebsocket[CommitWrapperEventModel]):
    @property
    def name(self) -> str:
        return "CreatePostStreamer"

    @property
    def stream_declaration(self) -> str:
        return """
        CREATE STREAM Bluesky {
            EVENT Post {
                did: string,
                kind: string,
                time: primary_time,
                time_str: string,
                cid: string,
                operation: string,
                record_type: string,
                langs: string,
                text: string
            },
            EVENT Follow {
                did: string,
                kind: string,
                time: primary_time,
                time_str: string,
                cid: string,
                operation: string,
                record_type: string,
                subject: string
            },
            EVENT Like {
                did: string,
                kind: string,
                time: primary_time,
                time_str: string,
                cid: string,
                operation: string,
                record_type: string,
                subject_cid: string,
                subject_uri: string
            },
            EVENT Repost {
                did: string,
                kind: string,
                time: primary_time,
                time_str: string,
                cid: string,
                operation: string,
                record_type: string,
                subject_cid: string,
                subject_uri: string
            }
        }
        """

    @property
    def option_declaration(self) -> Optional[str]:
        return """
                    CREATE QUARANTINE
                    { \n
                    BOUNDED_TIME 10 seconds {Bluesky} \n
                    }
                    """

    @property
    def URI(self) -> str:
        return "wss://jetstream2.us-west.bsky.network/subscribe"

    @property
    def subscribe_message_json(self) -> str:
        return ""

    def parse_message_json(self, message: str) -> Optional[CommitWrapperEventModel]:
        try:
            message_json = json.loads(message)
            model = CommitWrapperEventModel.model_validate(message_json)
            return model
        except Exception:
            return None

    def get_event_id_from_model(self, model: CommitWrapperEventModel) -> int:
        event_dict = {
            "app.bsky.feed.post": "Post",
            "app.bsky.graph.follow": "Follow",
            "app.bsky.feed.like": "Like",
            "app.bsky.feed.repost": "Repost",
        }
        event_name = event_dict.get(model.commit.record.record_type)
        assert event_name is not None, f"Unknown record type: {model.commit.record.record_type}"
        event_id = self.event_name_to_unique_id.get(event_name)
        assert event_id is not None, f"Unique ID for event {event_name} not found"
        return event_id

    def common_event_attributes(self, model: CommitWrapperEventModel):
        did = pycer.PyStringValue(model.did)
        kind = pycer.PyStringValue(model.kind)
        time = pycer.PyIntValue(int(model.commit.record.createdAt.timestamp() * 1e9))
        time_str = pycer.PyStringValue(model.commit.record.createdAt.__str__())
        cid = pycer.PyStringValue(model.commit.cid)
        operation = pycer.PyStringValue(model.commit.operation)
        record_type = pycer.PyStringValue(model.commit.record.record_type)
        return [did, kind, time, time_str, cid, operation, record_type]

    def record_subject_attribute(self, model: CommitWrapperEventModel):
        subject_cid = pycer.PyStringValue(model.commit.record.subject.cid)
        subject_uri = pycer.PyStringValue(model.commit.record.subject.uri)
        return [subject_cid, subject_uri]

    def event_post_attributes(self, model: CommitWrapperEventModel):
        langs = pycer.PyStringValue(", ".join(model.commit.record.langs))
        text = pycer.PyStringValue(model.commit.record.text)
        return [langs, text]

    def event_follow_attributes(self, model: CommitWrapperEventModel):
        subject = pycer.PyStringValue(model.commit.record.subject)
        return [subject]

    def event_like_attribute(self, model: CommitWrapperEventModel):
        return self.record_subject_attribute(model)

    def event_repost_attribute(self, model: CommitWrapperEventModel):
        return self.record_subject_attribute(model)

    def get_event_attributes(self, model: CommitWrapperEventModel):
        switcher = {
            "app.bsky.feed.post": self.event_post_attributes,
            "app.bsky.graph.follow": self.event_follow_attributes,
            "app.bsky.feed.like": self.event_like_attribute,
            "app.bsky.feed.repost": self.event_repost_attribute,
        }
        attributes = switcher.get(model.commit.record.record_type)
        assert attributes is not None, f"unknown record type: {model.commit.record.record_type}"
        return attributes(model)

    def create_event(self, model: CommitWrapperEventModel):
        attributes = self.common_event_attributes(model)
        event_attributes = self.get_event_attributes(model)
        attributes.extend(event_attributes)
        event_id = self.get_event_id_from_model(model)
        time = pycer.PyIntValue(int(model.commit.record.createdAt.timestamp() * 1e9))
        event = pycer.PyEvent(event_id, attributes, time)
        return event
