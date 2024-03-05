from pydantic import BaseModel

class SpaceObj(BaseModel):
    category: str
    projectName: str
    spaceName: str
    spaceFullName: str
    level: str
    area: str
    uniqueIdentifier: str
    spaceId: str
    bounds: str
    coordinates: str

class EquipmentObj(BaseModel):
    category: str
    equipmentName: str
    equipmentType: str
    description: str
    min: str
    max: str
    centroid: str
    level: str
    uniqueIdentifier: str