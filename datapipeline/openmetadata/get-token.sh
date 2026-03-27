#!/bin/bash
# Script para obter o token JWT do ingestion-bot no OpenMetadata

echo "Aguardando OpenMetadata iniciar..."
sleep 10

echo "Obtendo token do ingestion-bot..."
docker exec openmetadata_ingestion python3 << 'EOF'
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.entity.teams.user import User

server_config = OpenMetadataConnection(
    hostPort='http://openmetadata-server:8585/api',
    authProvider='openmetadata',
    securityConfig=OpenMetadataJWTClientConfig(
        jwtToken='eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJyb2xlcyI6WyJJbmdlc3Rpb25Cb3RSb2xlIl0sImVtYWlsIjoiaW5nZXN0aW9uLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJ1c2VybmFtZSI6ImluZ2VzdGlvbi1ib3QiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJpbmdlc3Rpb24tYm90IiwiaWF0IjoxNzc0NDczMDQ1LCJleHAiOm51bGx9.smaSoRNVq0wj2VFJcx1nZL5uOlxe1RASsViFbas_CDIyspx-JpMPt2GUDGmlnDZgCvACNEj50w8nRuZugCpZjJ5E4DRPX_SUebXItAaRnJBc0SkN3bTw9py3dCgUtAEAexRgDVt1xI8IfcDhpwq_ovJIGHgsRWdAxKLUMEU7A1N44q60bMvxn4MFC0bCQD557MxRVCUwu9jp-IQ7bpyfZ8n7r_6U0Ik3dA6EMBKl2YThhGV2GbluIdsp3QxlnxoSpIHKdRuuWEw-7YjdCabWey9e6S-_clZRyvJnlD8B_Rj2acr87r9sSP7nCtYHIt_Izyf_ryuxGplzgdZNx0VN9g'
    )
)

try:
    metadata = OpenMetadata(server_config)
    bot = metadata.get_by_name(entity=User, fqn='ingestion-bot')
    if bot and hasattr(bot, 'authenticationMechanism') and bot.authenticationMechanism:
        token = bot.authenticationMechanism.config.JWTToken
        print(f"\n✅ Token obtido com sucesso:\n{token}\n")
    else:
        print("❌ Bot não possui token configurado")
except Exception as e:
    print(f"❌ Erro: {e}")
EOF
