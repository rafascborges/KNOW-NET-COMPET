# Auto generated from schema.yaml by pythongen.py version: 0.0.1
# Generation date: 2025-12-22T12:16:25
# Schema: KnownetApplicationProfile
#
# id: https://example.org/knownet
# description:
# license: https://creativecommons.org/publicdomain/zero/1.0/

import dataclasses
import re
from dataclasses import dataclass
from datetime import (
    date,
    datetime,
    time
)
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Union
)

from jsonasobj2 import (
    JsonObj,
    as_dict
)
from linkml_runtime.linkml_model.meta import (
    EnumDefinition,
    PermissibleValue,
    PvFormulaOptions
)
from linkml_runtime.utils.curienamespace import CurieNamespace
from linkml_runtime.utils.enumerations import EnumDefinitionImpl
from linkml_runtime.utils.formatutils import (
    camelcase,
    sfx,
    underscore
)
from linkml_runtime.utils.metamodelcore import (
    bnode,
    empty_dict,
    empty_list
)
from linkml_runtime.utils.slot import Slot
from linkml_runtime.utils.yamlutils import (
    YAMLRoot,
    extended_float,
    extended_int,
    extended_str
)
from rdflib import (
    Namespace,
    URIRef
)

from linkml_runtime.linkml_model.types import Boolean, Date, Datetime, Float, Integer, String
from linkml_runtime.utils.metamodelcore import Bool, XSDDate, XSDDateTime

metamodel_version = "1.7.0"
version = None

# Namespaces
DATA = CurieNamespace('data', 'http://example.org/data/')
DCTERMS = CurieNamespace('dcterms', 'http://purl.org/dc/terms/')
EXT = CurieNamespace('ext', 'http://example.org/extensions/')
FOAF = CurieNamespace('foaf', 'http://xmlns.com/foaf/0.1/')
LINKML = CurieNamespace('linkml', 'https://w3id.org/linkml/')
OCDS = CurieNamespace('ocds', 'http://data.tbfy.eu/ontology/ocds#')
ORG = CurieNamespace('org', 'http://www.w3.org/ns/org#')
SCHEMA = CurieNamespace('schema', 'http://schema.org/')
SKOS = CurieNamespace('skos', 'http://www.w3.org/2004/02/skos/core#')
XSD = CurieNamespace('xsd', 'http://www.w3.org/2001/XMLSchema#')
DEFAULT_ = DATA


# Types

# Class references
class ContractId(extended_str):
    pass


class TenderId(extended_str):
    pass


class LocationId(extended_str):
    pass


class CPVId(extended_str):
    pass


class DocumentId(extended_str):
    pass


class EntityId(extended_str):
    pass


class RoleId(extended_str):
    pass


class PersonPersonId(extended_str):
    pass


@dataclass(repr=False)
class Contract(YAMLRoot):
    _inherited_slots: ClassVar[list[str]] = []

    class_class_uri: ClassVar[URIRef] = OCDS["Contract"]
    class_class_curie: ClassVar[str] = "ocds:Contract"
    class_name: ClassVar[str] = "Contract"
    class_model_uri: ClassVar[URIRef] = DATA.Contract

    id: Union[str, ContractId] = None
    executedAtLocation: Union[dict[Union[str, LocationId], Union[dict, "Location"]], list[Union[dict, "Location"]]] = empty_dict()
    signing_date: Union[str, XSDDateTime] = None
    initial_value: float = None
    final_value: float = None
    execution_deadline: int = None
    contract_type: Union[Union[str, "ContractTypeEnum"], list[Union[str, "ContractTypeEnum"]]] = None
    hasClassification: Optional[Union[Union[str, CPVId], list[Union[str, CPVId]]]] = empty_list()
    hasDocuments: Optional[Union[dict[Union[str, DocumentId], Union[dict, "Document"]], list[Union[dict, "Document"]]]] = empty_dict()
    causes_deadline_change: Optional[str] = None
    causes_price_change: Optional[str] = None

    def __post_init__(self, *_: str, **kwargs: Any):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, ContractId):
            self.id = ContractId(self.id)

        if self._is_empty(self.executedAtLocation):
            self.MissingRequiredField("executedAtLocation")
        self._normalize_inlined_as_dict(slot_name="executedAtLocation", slot_type=Location, key_name="id", keyed=True)

        if self._is_empty(self.signing_date):
            self.MissingRequiredField("signing_date")
        if not isinstance(self.signing_date, XSDDateTime):
            self.signing_date = XSDDateTime(self.signing_date)

        if self._is_empty(self.initial_value):
            self.MissingRequiredField("initial_value")
        if not isinstance(self.initial_value, float):
            self.initial_value = float(self.initial_value)

        if self._is_empty(self.final_value):
            self.MissingRequiredField("final_value")
        if not isinstance(self.final_value, float):
            self.final_value = float(self.final_value)

        if self._is_empty(self.execution_deadline):
            self.MissingRequiredField("execution_deadline")
        if not isinstance(self.execution_deadline, int):
            self.execution_deadline = int(self.execution_deadline)

        if self._is_empty(self.contract_type):
            self.MissingRequiredField("contract_type")
        if not isinstance(self.contract_type, list):
            self.contract_type = [self.contract_type] if self.contract_type is not None else []
        self.contract_type = [v if isinstance(v, ContractTypeEnum) else ContractTypeEnum(v) for v in self.contract_type]

        if not isinstance(self.hasClassification, list):
            self.hasClassification = [self.hasClassification] if self.hasClassification is not None else []
        self.hasClassification = [v if isinstance(v, CPVId) else CPVId(v) for v in self.hasClassification]

        self._normalize_inlined_as_dict(slot_name="hasDocuments", slot_type=Document, key_name="id", keyed=True)

        if self.causes_deadline_change is not None and not isinstance(self.causes_deadline_change, str):
            self.causes_deadline_change = str(self.causes_deadline_change)

        if self.causes_price_change is not None and not isinstance(self.causes_price_change, str):
            self.causes_price_change = str(self.causes_price_change)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Tender(YAMLRoot):
    _inherited_slots: ClassVar[list[str]] = []

    class_class_uri: ClassVar[URIRef] = OCDS["Tender"]
    class_class_curie: ClassVar[str] = "ocds:Tender"
    class_name: ClassVar[str] = "Tender"
    class_model_uri: ClassVar[URIRef] = DATA.Tender

    id: Union[str, TenderId] = None
    awardsContract: Union[str, ContractId] = None
    procurement_method: Union[str, "ProcurementMethodEnum"] = None
    procedure_type: str = None
    numberOfTenderers: Optional[int] = None
    publication_date: Optional[Union[str, XSDDateTime]] = None
    close_date: Optional[Union[str, XSDDateTime]] = None
    environmental_criteria: Optional[Union[bool, Bool]] = None
    material_criteria: Optional[Union[bool, Bool]] = None
    centralized_procedure: Optional[Union[bool, Bool]] = None

    def __post_init__(self, *_: str, **kwargs: Any):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, TenderId):
            self.id = TenderId(self.id)

        if self._is_empty(self.awardsContract):
            self.MissingRequiredField("awardsContract")
        if not isinstance(self.awardsContract, ContractId):
            self.awardsContract = ContractId(self.awardsContract)

        if self._is_empty(self.procurement_method):
            self.MissingRequiredField("procurement_method")
        if not isinstance(self.procurement_method, ProcurementMethodEnum):
            self.procurement_method = ProcurementMethodEnum(self.procurement_method)

        if self._is_empty(self.procedure_type):
            self.MissingRequiredField("procedure_type")
        if not isinstance(self.procedure_type, str):
            self.procedure_type = str(self.procedure_type)

        if self.numberOfTenderers is not None and not isinstance(self.numberOfTenderers, int):
            self.numberOfTenderers = int(self.numberOfTenderers)

        if self.publication_date is not None and not isinstance(self.publication_date, XSDDateTime):
            self.publication_date = XSDDateTime(self.publication_date)

        if self.close_date is not None and not isinstance(self.close_date, XSDDateTime):
            self.close_date = XSDDateTime(self.close_date)

        if self.environmental_criteria is not None and not isinstance(self.environmental_criteria, Bool):
            self.environmental_criteria = Bool(self.environmental_criteria)

        if self.material_criteria is not None and not isinstance(self.material_criteria, Bool):
            self.material_criteria = Bool(self.material_criteria)

        if self.centralized_procedure is not None and not isinstance(self.centralized_procedure, Bool):
            self.centralized_procedure = Bool(self.centralized_procedure)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Location(YAMLRoot):
    _inherited_slots: ClassVar[list[str]] = []

    class_class_uri: ClassVar[URIRef] = SCHEMA["PostalAddress"]
    class_class_curie: ClassVar[str] = "schema:PostalAddress"
    class_name: ClassVar[str] = "Location"
    class_model_uri: ClassVar[URIRef] = DATA.Location

    id: Union[str, LocationId] = None
    country: str = None
    district: Optional[str] = None
    municipality: Optional[str] = None

    def __post_init__(self, *_: str, **kwargs: Any):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, LocationId):
            self.id = LocationId(self.id)

        if self._is_empty(self.country):
            self.MissingRequiredField("country")
        if not isinstance(self.country, str):
            self.country = str(self.country)

        if self.district is not None and not isinstance(self.district, str):
            self.district = str(self.district)

        if self.municipality is not None and not isinstance(self.municipality, str):
            self.municipality = str(self.municipality)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class CPV(YAMLRoot):
    _inherited_slots: ClassVar[list[str]] = []

    class_class_uri: ClassVar[URIRef] = SKOS["Concept"]
    class_class_curie: ClassVar[str] = "skos:Concept"
    class_name: ClassVar[str] = "CPV"
    class_model_uri: ClassVar[URIRef] = DATA.CPV

    id: Union[str, CPVId] = None
    label: str = None
    level: str = None
    broader: Optional[Union[dict, "CPV"]] = None

    def __post_init__(self, *_: str, **kwargs: Any):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, CPVId):
            self.id = CPVId(self.id)

        if self._is_empty(self.label):
            self.MissingRequiredField("label")
        if not isinstance(self.label, str):
            self.label = str(self.label)

        if self._is_empty(self.level):
            self.MissingRequiredField("level")
        if not isinstance(self.level, str):
            self.level = str(self.level)

        if self.broader is not None and not isinstance(self.broader, CPV):
            self.broader = CPV(**as_dict(self.broader))

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Document(YAMLRoot):
    _inherited_slots: ClassVar[list[str]] = []

    class_class_uri: ClassVar[URIRef] = OCDS["Document"]
    class_class_curie: ClassVar[str] = "ocds:Document"
    class_name: ClassVar[str] = "Document"
    class_model_uri: ClassVar[URIRef] = DATA.Document

    id: Union[str, DocumentId] = None
    document_url: str = None
    document_description: Optional[str] = None

    def __post_init__(self, *_: str, **kwargs: Any):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, DocumentId):
            self.id = DocumentId(self.id)

        if self._is_empty(self.document_url):
            self.MissingRequiredField("document_url")
        if not isinstance(self.document_url, str):
            self.document_url = str(self.document_url)

        if self.document_description is not None and not isinstance(self.document_description, str):
            self.document_description = str(self.document_description)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Entity(YAMLRoot):
    _inherited_slots: ClassVar[list[str]] = []

    class_class_uri: ClassVar[URIRef] = ORG["Organization"]
    class_class_curie: ClassVar[str] = "org:Organization"
    class_name: ClassVar[str] = "Entity"
    class_model_uri: ClassVar[URIRef] = DATA.Entity

    id: Union[str, EntityId] = None
    hasRole: Optional[Union[dict[Union[str, RoleId], Union[dict, "Role"]], list[Union[dict, "Role"]]]] = empty_dict()
    isProcuringEntityFor: Optional[Union[Union[str, TenderId], list[Union[str, TenderId]]]] = empty_list()
    isTendererFor: Optional[Union[Union[str, TenderId], list[Union[str, TenderId]]]] = empty_list()
    wonTender: Optional[Union[Union[str, TenderId], list[Union[str, TenderId]]]] = empty_list()
    signedContract: Optional[Union[Union[str, ContractId], list[Union[str, ContractId]]]] = empty_list()
    entity_name: Optional[str] = None

    def __post_init__(self, *_: str, **kwargs: Any):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, EntityId):
            self.id = EntityId(self.id)

        self._normalize_inlined_as_dict(slot_name="hasRole", slot_type=Role, key_name="id", keyed=True)

        if not isinstance(self.isProcuringEntityFor, list):
            self.isProcuringEntityFor = [self.isProcuringEntityFor] if self.isProcuringEntityFor is not None else []
        self.isProcuringEntityFor = [v if isinstance(v, TenderId) else TenderId(v) for v in self.isProcuringEntityFor]

        if not isinstance(self.isTendererFor, list):
            self.isTendererFor = [self.isTendererFor] if self.isTendererFor is not None else []
        self.isTendererFor = [v if isinstance(v, TenderId) else TenderId(v) for v in self.isTendererFor]

        if not isinstance(self.wonTender, list):
            self.wonTender = [self.wonTender] if self.wonTender is not None else []
        self.wonTender = [v if isinstance(v, TenderId) else TenderId(v) for v in self.wonTender]

        if not isinstance(self.signedContract, list):
            self.signedContract = [self.signedContract] if self.signedContract is not None else []
        self.signedContract = [v if isinstance(v, ContractId) else ContractId(v) for v in self.signedContract]

        if self.entity_name is not None and not isinstance(self.entity_name, str):
            self.entity_name = str(self.entity_name)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Role(YAMLRoot):
    _inherited_slots: ClassVar[list[str]] = []

    class_class_uri: ClassVar[URIRef] = ORG["Role"]
    class_class_curie: ClassVar[str] = "org:Role"
    class_name: ClassVar[str] = "Role"
    class_model_uri: ClassVar[URIRef] = DATA.Role

    id: Union[str, RoleId] = None
    role_name: Optional[str] = None

    def __post_init__(self, *_: str, **kwargs: Any):
        if self._is_empty(self.id):
            self.MissingRequiredField("id")
        if not isinstance(self.id, RoleId):
            self.id = RoleId(self.id)

        if self.role_name is not None and not isinstance(self.role_name, str):
            self.role_name = str(self.role_name)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class TenurePeriod(YAMLRoot):
    _inherited_slots: ClassVar[list[str]] = []

    class_class_uri: ClassVar[URIRef] = ORG["Membership"]
    class_class_curie: ClassVar[str] = "org:Membership"
    class_name: ClassVar[str] = "TenurePeriod"
    class_model_uri: ClassVar[URIRef] = DATA.TenurePeriod

    inRole: Optional[Union[dict, Role]] = None
    start_date: Optional[Union[str, XSDDate]] = None
    end_date: Optional[Union[str, XSDDate]] = None

    def __post_init__(self, *_: str, **kwargs: Any):
        if self.inRole is not None and not isinstance(self.inRole, Role):
            self.inRole = Role(**as_dict(self.inRole))

        if self.start_date is not None and not isinstance(self.start_date, XSDDate):
            self.start_date = XSDDate(self.start_date)

        if self.end_date is not None and not isinstance(self.end_date, XSDDate):
            self.end_date = XSDDate(self.end_date)

        super().__post_init__(**kwargs)


@dataclass(repr=False)
class Person(YAMLRoot):
    _inherited_slots: ClassVar[list[str]] = []

    class_class_uri: ClassVar[URIRef] = FOAF["Agent"]
    class_class_curie: ClassVar[str] = "foaf:Agent"
    class_name: ClassVar[str] = "Person"
    class_model_uri: ClassVar[URIRef] = DATA.Person

    person_id: Union[str, PersonPersonId] = None
    isMember: Optional[Union[Union[dict, TenurePeriod], list[Union[dict, TenurePeriod]]]] = empty_list()
    person_name: Optional[str] = None

    def __post_init__(self, *_: str, **kwargs: Any):
        if self._is_empty(self.person_id):
            self.MissingRequiredField("person_id")
        if not isinstance(self.person_id, PersonPersonId):
            self.person_id = PersonPersonId(self.person_id)

        if not isinstance(self.isMember, list):
            self.isMember = [self.isMember] if self.isMember is not None else []
        self.isMember = [v if isinstance(v, TenurePeriod) else TenurePeriod(**as_dict(v)) for v in self.isMember]

        if self.person_name is not None and not isinstance(self.person_name, str):
            self.person_name = str(self.person_name)

        super().__post_init__(**kwargs)


# Enumerations
class ContractTypeEnum(EnumDefinitionImpl):

    Sociedade = PermissibleValue(text="Sociedade")

    _defn = EnumDefinition(
        name="ContractTypeEnum",
    )

    @classmethod
    def _addvals(cls):
        setattr(cls, "Concessão de obras públicas",
            PermissibleValue(text="Concessão de obras públicas"))
        setattr(cls, "Concessão de serviços públicos",
            PermissibleValue(text="Concessão de serviços públicos"))
        setattr(cls, "Aquisição de serviços",
            PermissibleValue(text="Aquisição de serviços"))
        setattr(cls, "Aquisição de bens móveis",
            PermissibleValue(text="Aquisição de bens móveis"))
        setattr(cls, "Empreitadas de obras públicas",
            PermissibleValue(text="Empreitadas de obras públicas"))
        setattr(cls, "Locação de bens móveis",
            PermissibleValue(text="Locação de bens móveis"))
        setattr(cls, "Outros Tipos",
            PermissibleValue(text="Outros Tipos"))

class ProcedureTypeEnum(EnumDefinitionImpl):

    _defn = EnumDefinition(
        name="ProcedureTypeEnum",
    )

    @classmethod
    def _addvals(cls):
        setattr(cls, "Concurso público simplificado",
            PermissibleValue(text="Concurso público simplificado"))
        setattr(cls, "Consulta prévia ao abrigo do artigo 7º da Lei n.º 30/2021, de 21.05",
            PermissibleValue(text="Consulta prévia ao abrigo do artigo 7º da Lei n.º 30/2021, de 21.05"))
        setattr(cls, "Serviços sociais e outros serviços específicos",
            PermissibleValue(text="Serviços sociais e outros serviços específicos"))
        setattr(cls, "Parceria para a inovação",
            PermissibleValue(text="Parceria para a inovação"))
        setattr(cls, "Consulta Prévia",
            PermissibleValue(text="Consulta Prévia"))
        setattr(cls, "Consulta Prévia Simplificada",
            PermissibleValue(text="Consulta Prévia Simplificada"))
        setattr(cls, "Ajuste Direto Regime Geral ao abrigo do artigo 7º da Lei n.º 30/2021, de 21.05",
            PermissibleValue(text="Ajuste Direto Regime Geral ao abrigo do artigo 7º da Lei n.º 30/2021, de 21.05"))
        setattr(cls, "Contratação excluída II",
            PermissibleValue(text="Contratação excluída II"))
        setattr(cls, "Ao abrigo de acordo-quadro (art.º 258.º)",
            PermissibleValue(text="Ao abrigo de acordo-quadro (art.º 258.º)"))
        setattr(cls, "Concurso de conceção simplificado",
            PermissibleValue(text="Concurso de conceção simplificado"))
        setattr(cls, "Setores especiais – isenção parte II",
            PermissibleValue(text="Setores especiais – isenção parte II"))
        setattr(cls, "Concurso público",
            PermissibleValue(text="Concurso público"))
        setattr(cls, "Ao abrigo de acordo-quadro (art.º 259.º)",
            PermissibleValue(text="Ao abrigo de acordo-quadro (art.º 259.º)"))
        setattr(cls, "Procedimento de negociação",
            PermissibleValue(text="Procedimento de negociação"))
        setattr(cls, "Concurso limitado por prévia qualificação",
            PermissibleValue(text="Concurso limitado por prévia qualificação"))
        setattr(cls, "Ajuste Direto Regime Geral",
            PermissibleValue(text="Ajuste Direto Regime Geral"))

class ProcurementMethodEnum(EnumDefinitionImpl):

    open = PermissibleValue(text="open")
    limited = PermissibleValue(text="limited")
    selective = PermissibleValue(text="selective")
    direct = PermissibleValue(text="direct")

    _defn = EnumDefinition(
        name="ProcurementMethodEnum",
    )

# Slots
class slots:
    pass

slots.awardsContract = Slot(uri=EXT.awardsContract, name="awardsContract", curie=EXT.curie('awardsContract'),
                   model_uri=DATA.awardsContract, domain=Tender, range=Union[str, ContractId])

slots.executedAtLocation = Slot(uri=SCHEMA.location, name="executedAtLocation", curie=SCHEMA.curie('location'),
                   model_uri=DATA.executedAtLocation, domain=Contract, range=Union[dict[Union[str, LocationId], Union[dict, "Location"]], list[Union[dict, "Location"]]])

slots.hasClassification = Slot(uri=OCDS.hasClassification, name="hasClassification", curie=OCDS.curie('hasClassification'),
                   model_uri=DATA.hasClassification, domain=Contract, range=Optional[Union[Union[str, CPVId], list[Union[str, CPVId]]]])

slots.broader = Slot(uri=SKOS.broader, name="broader", curie=SKOS.curie('broader'),
                   model_uri=DATA.broader, domain=CPV, range=Optional[Union[dict, "CPV"]])

slots.hasDocuments = Slot(uri=OCDS.hasDocument, name="hasDocuments", curie=OCDS.curie('hasDocument'),
                   model_uri=DATA.hasDocuments, domain=Contract, range=Optional[Union[dict[Union[str, DocumentId], Union[dict, "Document"]], list[Union[dict, "Document"]]]])

slots.hasRole = Slot(uri=EXT.hasRole, name="hasRole", curie=EXT.curie('hasRole'),
                   model_uri=DATA.hasRole, domain=Entity, range=Optional[Union[dict[Union[str, RoleId], Union[dict, "Role"]], list[Union[dict, "Role"]]]])

slots.isProcuringEntityFor = Slot(uri=OCDS.isProcuringEntityFor, name="isProcuringEntityFor", curie=OCDS.curie('isProcuringEntityFor'),
                   model_uri=DATA.isProcuringEntityFor, domain=Entity, range=Optional[Union[Union[str, TenderId], list[Union[str, TenderId]]]])

slots.isTendererFor = Slot(uri=OCDS.isTendererFor, name="isTendererFor", curie=OCDS.curie('isTendererFor'),
                   model_uri=DATA.isTendererFor, domain=Entity, range=Optional[Union[Union[str, TenderId], list[Union[str, TenderId]]]])

slots.wonTender = Slot(uri=EXT.wonTender, name="wonTender", curie=EXT.curie('wonTender'),
                   model_uri=DATA.wonTender, domain=Entity, range=Optional[Union[Union[str, TenderId], list[Union[str, TenderId]]]])

slots.signedContract = Slot(uri=OCDS.isSignatoryForContract, name="signedContract", curie=OCDS.curie('isSignatoryForContract'),
                   model_uri=DATA.signedContract, domain=Entity, range=Optional[Union[Union[str, ContractId], list[Union[str, ContractId]]]])

slots.inRole = Slot(uri=ORG.role, name="inRole", curie=ORG.curie('role'),
                   model_uri=DATA.inRole, domain=TenurePeriod, range=Optional[Union[dict, Role]])

slots.isMember = Slot(uri=ORG.member, name="isMember", curie=ORG.curie('member'),
                   model_uri=DATA.isMember, domain=Person, range=Optional[Union[Union[dict, TenurePeriod], list[Union[dict, TenurePeriod]]]])

slots.contract__id = Slot(uri=OCDS.contractId, name="contract__id", curie=OCDS.curie('contractId'),
                   model_uri=DATA.contract__id, domain=None, range=URIRef)

slots.contract__signing_date = Slot(uri=OCDS.dateContractSigned, name="contract__signing_date", curie=OCDS.curie('dateContractSigned'),
                   model_uri=DATA.contract__signing_date, domain=None, range=Union[str, XSDDateTime])

slots.contract__initial_value = Slot(uri=EXT.initialContractValue, name="contract__initial_value", curie=EXT.curie('initialContractValue'),
                   model_uri=DATA.contract__initial_value, domain=None, range=float)

slots.contract__final_value = Slot(uri=EXT.finalContractValue, name="contract__final_value", curie=EXT.curie('finalContractValue'),
                   model_uri=DATA.contract__final_value, domain=None, range=float)

slots.contract__execution_deadline = Slot(uri=EXT.durationInDays, name="contract__execution_deadline", curie=EXT.curie('durationInDays'),
                   model_uri=DATA.contract__execution_deadline, domain=None, range=int)

slots.contract__contract_type = Slot(uri=EXT.contractType, name="contract__contract_type", curie=EXT.curie('contractType'),
                   model_uri=DATA.contract__contract_type, domain=None, range=Union[Union[str, "ContractTypeEnum"], list[Union[str, "ContractTypeEnum"]]])

slots.contract__causes_deadline_change = Slot(uri=EXT.causesDeadlineChange, name="contract__causes_deadline_change", curie=EXT.curie('causesDeadlineChange'),
                   model_uri=DATA.contract__causes_deadline_change, domain=None, range=Optional[str])

slots.contract__causes_price_change = Slot(uri=EXT.causesPriceChange, name="contract__causes_price_change", curie=EXT.curie('causesPriceChange'),
                   model_uri=DATA.contract__causes_price_change, domain=None, range=Optional[str])

slots.tender__id = Slot(uri=OCDS.tenderId, name="tender__id", curie=OCDS.curie('tenderId'),
                   model_uri=DATA.tender__id, domain=None, range=URIRef)

slots.tender__numberOfTenderers = Slot(uri=OCDS.numberOfTenderers, name="tender__numberOfTenderers", curie=OCDS.curie('numberOfTenderers'),
                   model_uri=DATA.tender__numberOfTenderers, domain=None, range=Optional[int])

slots.tender__procurement_method = Slot(uri=OCDS.procurementMethod, name="tender__procurement_method", curie=OCDS.curie('procurementMethod'),
                   model_uri=DATA.tender__procurement_method, domain=None, range=Union[str, "ProcurementMethodEnum"])

slots.tender__procedure_type = Slot(uri=EXT.procedureType, name="tender__procedure_type", curie=EXT.curie('procedureType'),
                   model_uri=DATA.tender__procedure_type, domain=None, range=str)

slots.tender__publication_date = Slot(uri=EXT.dateTenderPublished, name="tender__publication_date", curie=EXT.curie('dateTenderPublished'),
                   model_uri=DATA.tender__publication_date, domain=None, range=Optional[Union[str, XSDDateTime]])

slots.tender__close_date = Slot(uri=EXT.dateTenderClosed, name="tender__close_date", curie=EXT.curie('dateTenderClosed'),
                   model_uri=DATA.tender__close_date, domain=None, range=Optional[Union[str, XSDDateTime]])

slots.tender__environmental_criteria = Slot(uri=EXT.environmentalCriteria, name="tender__environmental_criteria", curie=EXT.curie('environmentalCriteria'),
                   model_uri=DATA.tender__environmental_criteria, domain=None, range=Optional[Union[bool, Bool]])

slots.tender__material_criteria = Slot(uri=EXT.materialCriteria, name="tender__material_criteria", curie=EXT.curie('materialCriteria'),
                   model_uri=DATA.tender__material_criteria, domain=None, range=Optional[Union[bool, Bool]])

slots.tender__centralized_procedure = Slot(uri=EXT.centralizedProcedure, name="tender__centralized_procedure", curie=EXT.curie('centralizedProcedure'),
                   model_uri=DATA.tender__centralized_procedure, domain=None, range=Optional[Union[bool, Bool]])

slots.location__id = Slot(uri=DATA.id, name="location__id", curie=DATA.curie('id'),
                   model_uri=DATA.location__id, domain=None, range=URIRef)

slots.location__country = Slot(uri=SCHEMA.addressCountry, name="location__country", curie=SCHEMA.curie('addressCountry'),
                   model_uri=DATA.location__country, domain=None, range=str)

slots.location__district = Slot(uri=SCHEMA.addressRegion, name="location__district", curie=SCHEMA.curie('addressRegion'),
                   model_uri=DATA.location__district, domain=None, range=Optional[str])

slots.location__municipality = Slot(uri=SCHEMA.addressLocality, name="location__municipality", curie=SCHEMA.curie('addressLocality'),
                   model_uri=DATA.location__municipality, domain=None, range=Optional[str])

slots.cPV__id = Slot(uri=SKOS.notation, name="cPV__id", curie=SKOS.curie('notation'),
                   model_uri=DATA.cPV__id, domain=None, range=URIRef)

slots.cPV__label = Slot(uri=SKOS.prefLabel, name="cPV__label", curie=SKOS.curie('prefLabel'),
                   model_uri=DATA.cPV__label, domain=None, range=str)

slots.cPV__level = Slot(uri=EXT.level, name="cPV__level", curie=EXT.curie('level'),
                   model_uri=DATA.cPV__level, domain=None, range=str)

slots.document__id = Slot(uri=OCDS.documentId, name="document__id", curie=OCDS.curie('documentId'),
                   model_uri=DATA.document__id, domain=None, range=URIRef)

slots.document__document_url = Slot(uri=SCHEMA.url, name="document__document_url", curie=SCHEMA.curie('url'),
                   model_uri=DATA.document__document_url, domain=None, range=str)

slots.document__document_description = Slot(uri=SCHEMA.description, name="document__document_description", curie=SCHEMA.curie('description'),
                   model_uri=DATA.document__document_description, domain=None, range=Optional[str])

slots.entity__id = Slot(uri=ORG.identifier, name="entity__id", curie=ORG.curie('identifier'),
                   model_uri=DATA.entity__id, domain=None, range=URIRef)

slots.entity__entity_name = Slot(uri=ORG.name, name="entity__entity_name", curie=ORG.curie('name'),
                   model_uri=DATA.entity__entity_name, domain=None, range=Optional[str])

slots.role__id = Slot(uri=ORG.identifier, name="role__id", curie=ORG.curie('identifier'),
                   model_uri=DATA.role__id, domain=None, range=URIRef)

slots.role__role_name = Slot(uri=ORG.name, name="role__role_name", curie=ORG.curie('name'),
                   model_uri=DATA.role__role_name, domain=None, range=Optional[str])

slots.tenurePeriod__start_date = Slot(uri=SCHEMA.startDate, name="tenurePeriod__start_date", curie=SCHEMA.curie('startDate'),
                   model_uri=DATA.tenurePeriod__start_date, domain=None, range=Optional[Union[str, XSDDate]])

slots.tenurePeriod__end_date = Slot(uri=SCHEMA.endDate, name="tenurePeriod__end_date", curie=SCHEMA.curie('endDate'),
                   model_uri=DATA.tenurePeriod__end_date, domain=None, range=Optional[Union[str, XSDDate]])

slots.person__person_id = Slot(uri=FOAF.identifier, name="person__person_id", curie=FOAF.curie('identifier'),
                   model_uri=DATA.person__person_id, domain=None, range=URIRef)

slots.person__person_name = Slot(uri=FOAF.name, name="person__person_name", curie=FOAF.curie('name'),
                   model_uri=DATA.person__person_name, domain=None, range=Optional[str])

