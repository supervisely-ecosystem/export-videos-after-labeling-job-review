import os
import supervisely as sly

from dotenv import load_dotenv
from tqdm import tqdm

from src.export_videos import export_videos

if sly.is_development():
    load_dotenv("local.env")
    load_dotenv(os.path.expanduser("~/supervisely.env"))

api = sly.Api.from_env()

JOB_ID = os.environ.get("modal.state.slyJobId")

app = sly.Application()


if JOB_ID is None:
    raise RuntimeError("Job ID is not specified")

job = api.labeling_job.get_info_by_id(JOB_ID)
project = api.project.get_info_by_id(job.project_id)
dataset = api.dataset.get_info_by_id(job.dataset_id)

if job is None or dataset is None or project is None:
    raise RuntimeError("Job, project or dataset not found")

reviewed_item_ids = [video["id"] for video in job.entities if video["reviewStatus"] == "accepted"]
sly.logger.info(f"Found {len(reviewed_item_ids)} reviewed videos")

# make project directory path
STORAGE_DIR = sly.app.get_data_dir()
project_dir = os.path.join(STORAGE_DIR, f"{project.id}_{project.name}")

# get project meta
meta_json = api.project.get_meta(id=project.id)
project_meta = sly.ProjectMeta.from_json(meta_json)


if project.type == str(sly.ProjectType.VIDEOS):
    export_videos(api, dataset, reviewed_item_ids, project_dir, project_meta)


reviewed = len(reviewed_item_ids)
not_reviewed = dataset.items_count - reviewed
items_type = " ".join(project.type.split("_"))
sly.logger.info(
    f"""
Dataset {dataset.name} has {dataset.items_count} {items_type}:
    * {reviewed} reviewed {items_type} - processed;
    * {not_reviewed} not reviewed {items_type} - skipped.
"""
)

app.stop()
