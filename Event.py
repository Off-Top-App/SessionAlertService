class Event:
    def __init__(self, user_id, focus_score, time):
        self.user_id= user_id
        self.focus_score= focus_score
        self.time= time

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "focus_score": self.focus_score,
            "time": self.time
        }
