from pydantic import BaseModel, Field

providence_id = 'providence-server-sso-54545-de68-9703-8hsdkg'

class UrlRedirect(BaseModel):
    Links: list = Field(None, description="Original URLs")

class AttachmentRequest(BaseModel):
    Links: list = Field(None, description="Source URLs")
    hash_input: bool = Field(True, description="hash input URLs")
    metadata: dict = Field({}, description="metadata to be added to mapping")


class B64Request(BaseModel):
    encodedUrls: list = Field(None, description="List of dicts containing source URL and b64 image")
    hash_input: bool = Field(True, description="hash input URLs")
    metadata: dict = Field({}, description="metadata to be added to mapping")


class crawlProfilePicModel(BaseModel):
    profile_id: str = Field(None, description="Profile ID from the Entity Card")
    profile_pic_url: str = Field(None, description="Profile picture URL from the Entity Card")
    social_media: str = Field("facebook", description="social media name")
    metadata: dict = Field({}, description="metadata to be added to mapping")