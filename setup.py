from setuptools import find_packages, setup

setup(
    name="availability",
    version="1.0.0",
    package_dir={"": "src"},
    packages=find_packages("src"),
    entry_points={
        "console_scripts": [
            "availability-monitor=availability.monitor:monitor",
            "availability-writer=availability.writer:writer",
        ]
    },
)
