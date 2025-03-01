# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2021 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

import pytest
from docker.models.containers import ContainerCollection

from nautilus_trader.adapters.interactive_brokers.config import DockerizedIBGatewayConfig
from nautilus_trader.adapters.interactive_brokers.gateway import DockerizedIBGateway


# Docker integration tests

pytestmark = pytest.mark.skip(reason="Skip due docker dependency")
def test_gateway_start_no_container(mocker):
    # Arrange
    # Mock the Docker module to avoid actual docker connections
    mock_docker_client = mocker.MagicMock()
    mock_containers = mocker.MagicMock()
    mock_containers.list.return_value = []
    mock_containers.run = mocker.MagicMock()
    mock_docker_client.containers = mock_containers
    
    # Mock the docker.from_env() call
    docker_from_env = mocker.patch('docker.from_env', return_value=mock_docker_client)
    
    config = DockerizedIBGatewayConfig(
        username="test",
        password="test",
    )
    gateway = DockerizedIBGateway(config=config)

    # Act
    gateway.start(wait=None)

    # Assert
    # Verify that the docker client's run method was called with the correct args
    call_args = mock_containers.run.call_args
    kwargs = call_args.kwargs
    
    # Check that the essential parameters were passed correctly
    assert kwargs["image"] == "ghcr.io/unusualalpha/ib-gateway"
    assert kwargs["name"] == f"{DockerizedIBGateway.CONTAINER_NAME}-4002"
    assert kwargs["detach"] is True
    assert "4002" in kwargs["ports"]
    assert kwargs["environment"]["TWS_USERID"] == "test"
    assert kwargs["environment"]["TWS_PASSWORD"] == "test"
    assert kwargs["environment"]["TRADING_MODE"] == "paper"
    assert kwargs["environment"]["READ_ONLY_API"] == "yes"
