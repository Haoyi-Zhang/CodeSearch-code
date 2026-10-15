"""Benign temporary-layout checks for the optional project packaging gate."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import validate_release


class ProjectLayoutTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.project = Path(self.directory.name)
        self.artifact = self.project / "artifact"
        self.artifact.mkdir()
        (self.project / "paper").mkdir()
        (self.artifact / "docs").mkdir()
        (self.artifact / "README.md").write_text("Artifact usage\n", encoding="utf-8")
        (self.artifact / "docs/third-party.md").write_text("License notices\n", encoding="utf-8")
        self.patch = patch.object(validate_release, "ROOT", self.artifact)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def test_two_directory_project_and_optional_root_readme(self):
        self.assertEqual(["artifact", "paper"],
                         validate_release.validate_hygiene(self.project)["project_root_entries"])
        (self.project / "README.md").write_text("Project usage\n", encoding="utf-8")
        self.assertEqual(["README.md", "artifact", "paper"],
                         validate_release.validate_hygiene(self.project)["project_root_entries"])

    def test_unexpected_root_file_is_still_rejected(self):
        (self.project / "unexpected.txt").write_text("unused\n", encoding="utf-8")
        with self.assertRaises(validate_release.ValidationError):
            validate_release.validate_hygiene(self.project)

    def test_required_directory_and_optional_readme_types_are_checked(self):
        (self.project / "paper").rmdir()
        (self.project / "paper").write_text("not a directory\n", encoding="utf-8")
        with self.assertRaises(validate_release.ValidationError):
            validate_release.validate_hygiene(self.project)
        (self.project / "paper").unlink()
        (self.project / "paper").mkdir()
        (self.project / "README.md").mkdir()
        with self.assertRaises(validate_release.ValidationError):
            validate_release.validate_hygiene(self.project)


if __name__ == "__main__":
    unittest.main()
