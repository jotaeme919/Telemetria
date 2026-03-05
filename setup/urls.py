"""
URL configuration for setup project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from rest_framework import routers, permissions
from api_telemetria.api import viewsets
from drf_yasg import openapi
from drf_yasg.views import get_schema_view

schema_view = get_schema_view(
    openapi.Info(
        title='API de Telemetria - Veículos Agrícolas',
        default_version='v1',
        description='Sistema para cadastro e controle por telemetria de frota de veículos agrícolas',
        terms_of_service='https://www.google.com/policies/terms/',
        contact=openapi.Contact(email='contato@telemetria.com.br'),
        license=openapi.License(name='OpenSource'),
    ),
    public=True,
    permission_classes=[permissions.AllowAny],
)

router = routers.DefaultRouter()
router.register(r'Veículos', viewsets.VeiculoViewSet, basename='veiculo')
router.register(r'Marcas', viewsets.MarcaViewSet, basename='marca')
router.register(r'Modelos', viewsets.ModeloViewSet, basename='modelo')
router.register(r'MedicoesVeiculos', viewsets.MedicaoVeiculoViewSet, basename='medicao_veiculo')
router.register(r'Medicoes', viewsets.MedicaoViewSet, basename='medicao')
router.register(r'UnidadesMedida', viewsets.UnidadeMedidaViewSet, basename='unidade_medida')

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include(router.urls)),
]

urlpatterns += [
    path('swaggrjson/', schema_view.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),
    path('swagger/', schema_view.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),
    path('redoc/', schema_view.with_ui('redoc', cache_timeout=0), name='schema-redoc'),
]