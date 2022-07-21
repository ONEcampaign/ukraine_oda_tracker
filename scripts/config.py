"""Handle paths to files and working directories"""
import os


class Paths:
    def __init__(self, project_dir):
        self.project_dir = project_dir

    @property
    def scripts(self):
        return os.path.join(self.project_dir, "scripts")

    @property
    def output(self):
        return os.path.join(self.project_dir, "output")

    @property
    def data(self):
        return os.path.join(
            self.project_dir,
            "raw_data",
        )


# Variable with paths as properties
PATHS = Paths(os.path.dirname(os.path.dirname(__file__)))
