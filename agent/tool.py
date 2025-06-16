from lib.academy.academy.behavior import Behavior, action, loop
from typing import List
from utils.schema import Tool
import subprocess
import json


class RheaToolAgent(Behavior):
    def __init__(self, tool: Tool) -> None:
        super().__init__()
        self.tool: Tool = tool
        self.python_verion: str = "3.8"
        self.installed_packages: List[str]
    
    def on_setup(self) -> None:
        # Create Conda environment
        cmd = ["conda", "create", "-n", self.tool.id, f"python={self.python_verion}", "-y"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Error creating Conda environment: {result.stdout}")

        # Install Conda packages
        requirements = self.tool.requirements.requirements
        packages = []
        for requirement in requirements:
            if requirement.type == "package":
                packages.append(f"{requirement.value}={requirement.version}")
            else:
                raise NotImplementedError(f'Requirement type of "{requirement.type}" not yet implemented.')
        cmd = ["conda", "install", "-n", self.tool.id, "-y"] + packages
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Error installing Conda packages: {result.stdout}")
        
        # List installed packages and parse into installed_packages
        cmd = ["conda", "list", "-n", self.tool.id, "--json"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Error listing Conda packages: {result.stdout}")
        pkg_info = json.loads(result.stdout)
        self.installed_packages = [f"{p['name']}={p['version']}" for p in pkg_info]


    def on_shutdown(self) -> None:
        # Delete Conda environment
        cmd = ["conda", "env", "remove", "-n", self.tool.id]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Error deleting Conda environment: {result.stdout}")
        
    @action
    def get_installed_packages(self) -> List[str]:
        cmd = ["conda", "list", "-n", self.tool.id, "--json"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Error listing Conda packages: {result.stdout}")
        pkg_info = json.loads(result.stdout)
        packages = [f"{p['name']}={p['version']}" for p in pkg_info]
        return packages