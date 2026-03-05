from rest_framework import viewsets
from api_telemetria import models
from api_telemetria.api import serializers
from drf_yasg.utils import swagger_auto_schema

class VeiculoViewSet(viewsets.ModelViewSet):
    queryset = models.Veiculo.objects.all()
    serializer_class = serializers.VeiculoSerializer
    
    # decoradores para documentação do swagger, descrevendo cada endpoint e os tipos de resposta esperados  
    
    @swagger_auto_schema( 
        operation_description="Retorna todas os tipos de veículos",
        responses={200: serializers.VeiculoSerializer(many=True)},
    )
    def list(self, request, *args, **kwargs):
        
        return super().list(request, *args, **kwargs)
    # Retorna todas os tipos de veículos, utilizando o serializer para formatar a resposta, e o decorador para documentar a operação no swagger
    # Método GET, endpoint /veiculo/ (listagem de veículos)
    
    @swagger_auto_schema(
        operation_description="Cria um novo registro de veículo",
        responses={201: serializers.VeiculoSerializer(many=True)},
    )
    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)
    # Cria um novo registro de veículo, utilizando o serializer para validar e salvar os dados, e o decorador para documentar a operação no swagger
    # Método POST, endpoint /veiculo/ (criação de veículo)
    
    @swagger_auto_schema(
        operation_description="Retorna o resgistro de veículo conforme o ID fornecido",
        responses={200: serializers.VeiculoSerializer(many=False)},
    )
    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)
    # Retorna o registro de veículo conforme o ID fornecido, utilizando o serializer para formatar a resposta, e o decorador para documentar a operação no swagger
    # Método GET ID, endpoint /veiculo/{id}/ (detalhes de um veículo específico)
    
    @swagger_auto_schema(
        operation_description="Altera o registro de veículo conforme o dados passados para o ID fornecido",
        responses={200: serializers.VeiculoSerializer(many=True)},
    )
    
    def update(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)
    # Altera o registro de veículo conforme os dados passados para o ID fornecido, utilizando o serializer para validar e salvar os dados, e o decorador para documentar a operação no swagger
    # Método PUT ID, endpoint /veiculo/{id}/ (atualização de um veículo específico)
    
    @swagger_auto_schema(
        operation_description="Deleta o registro de veículo conforme o ID fornecido",
        responses={204: serializers.VeiculoSerializer(many=True)},
    )
    def destroy(self, request, *args, **kwargs):
        return super().destroy(request, *args, **kwargs)
    # Deleta o registro de veículo conforme o ID fornecido, utilizando o decorador para documentar a operação no swagger
    # Método DELETE ID, endpoint /veiculo/{id}/ (remoção de um veículo específico)
    
class MarcaViewSet(viewsets.ModelViewSet):
    queryset = models.Marca.objects.all()
    serializer_class = serializers.MarcaSerializer
    @swagger_auto_schema(
        operation_description="Retorna todas os tipos de marcas de veículos",
        responses={200: serializers.MarcaSerializer(many=True)},
    )
    def list(self, request, *args, **kwargs):
        
        return super().list(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Cria um novo registro de marca de veículo",
        responses={201: serializers.MarcaSerializer(many=True)},
    )
    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Retorna o resgistro de marca conforme o ID fornecido",
        responses={200: serializers.MarcaSerializer(many=False)},
    )
    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Altera o registro de marca conforme o dados passados para o ID fornecido",
        responses={200: serializers.MarcaSerializer(many=True)},
    )
    
    def update(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Deleta o registro de marca conforme o ID fornecido",
        responses={204: serializers.MarcaSerializer(many=True)},
    )
    def destroy(self, request, *args, **kwargs):
        return super().destroy(request, *args, **kwargs)

class ModeloViewSet(viewsets.ModelViewSet):
    queryset = models.Modelo.objects.all()
    serializer_class = serializers.ModeloSerializer
    
    @swagger_auto_schema(
        operation_description="Retorna todas os tipos de modelos de veículos",
        responses={200: serializers.ModeloSerializer(many=True)},
    )
    def list(self, request, *args, **kwargs):
        
        return super().list(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Cria um novo registro de modelo de veículo",
        responses={201: serializers.ModeloSerializer(many=True)},
    )
    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Retorna o resgistro de modelo de veículo conforme o ID fornecido",
        responses={200: serializers.ModeloSerializer(many=True)},
    )
    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Altera o registro de modelo de veículo conforme o dados passados para o ID fornecido",
        responses={200: serializers.ModeloSerializer(many=True)},
    )
    
    def update(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Deleta o registro de modelo de veículo conforme o ID fornecido",
        responses={204: serializers.ModeloSerializer(many=True)},
    )
    def destroy(self, request, *args, **kwargs):
        return super().destroy(request, *args, **kwargs)

class MedicaoVeiculoViewSet(viewsets.ModelViewSet):
    queryset = models.MedicaoVeiculo.objects.all()
    serializer_class = serializers.MedicaoVeiculoSerializer
    
    @swagger_auto_schema(
        operation_description="Retorna todas os tipos de medição de veículos específicos",
        responses={200: serializers.MedicaoVeiculoSerializer(many=True)},
    )
    def list(self, request, *args, **kwargs):
        
        return super().list(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Cria um novo registro de medição de veículos específicos",
        responses={201: serializers.MedicaoVeiculoSerializer(many=True)},
    )
    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Retorna o resgistro de medição de veículos específicos conforme o ID fornecido",
        responses={200: serializers.MedicaoVeiculoSerializer(many=True)},
    )
    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Altera o registro de medição de veículos específicos conforme o dados passados para o ID fornecido",
        responses={200: serializers.MedicaoVeiculoSerializer(many=True)},
    )
    
    def update(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Deleta o registro de medição de veículos específicos conforme o ID fornecido",
        responses={204: serializers.MedicaoVeiculoSerializer(many=True)},
    )
    def destroy(self, request, *args, **kwargs):
        return super().destroy(request, *args, **kwargs)

class MedicaoViewSet(viewsets.ModelViewSet):
    queryset = models.Medicao.objects.all()
    serializer_class = serializers.MedicaoSerializer
    @swagger_auto_schema(
        operation_description="Retorna todas as informações de telemetria de tipos de medição",
        responses={200: serializers.MedicaoSerializer(many=True)},
    )
    def list(self, request, *args, **kwargs):
        
        return super().list(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Cria um novo registro de medição de telemetria",
        responses={201: serializers.MedicaoSerializer(many=True)},
    )
    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Retorna o resgistro de medição conforme o ID fornecido",
        responses={200: serializers.MedicaoSerializer(many=True)},
    )
    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Altera o registro de medição conforme o dados passados para o ID fornecido",
        responses={200: serializers.MedicaoSerializer(many=True)},
    )
    
    def update(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Deleta o registro de medição conforme o ID fornecido",
        responses={204: serializers.MedicaoSerializer(many=True)},
    )
    def destroy(self, request, *args, **kwargs):
        return super().destroy(request, *args, **kwargs)
    
class UnidadeMedidaViewSet(viewsets.ModelViewSet):
    queryset = models.UnidadeMedida.objects.all()
    serializer_class = serializers.UnidadeMedidaSerializer
    
    @swagger_auto_schema(
        operation_description="Retorna todas os tipos de unidade de medida",
        responses={200: serializers.UnidadeMedidaSerializer(many=True)},
    )
    def list(self, request, *args, **kwargs):
        
        return super().list(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Cria um novo registro de unidade de medida",
        responses={201: serializers.UnidadeMedidaSerializer(many=True)},
    )
    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Retorna o resgistro de unidade de medida conforme o ID fornecido",
        responses={200: serializers.UnidadeMedidaSerializer(many=True)},
    )
    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Altera o registro de unidade de medida conforme o dados passados para o ID fornecido",
        responses={200: serializers.UnidadeMedidaSerializer(many=True)},
    )
    
    def update(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)
    
    @swagger_auto_schema(
        operation_description="Deleta o registro de medição conforme o ID fornecido",
        responses={204: serializers.MedicaoSerializer(many=True)},
    )
    def destroy(self, request, *args, **kwargs):
        return super().destroy(request, *args, **kwargs)