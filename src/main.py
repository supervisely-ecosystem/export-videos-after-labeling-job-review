import os
import supervisely as sly

from dotenv import load_dotenv
from tqdm import tqdm

from src.export_videos import export_videos
from src.export_images import export_images
from src.export_pointclouds import export_pointclouds

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

items_type = " ".join(project.type.split("_"))
reviewed_item_ids = [item["id"] for item in job.entities if item["reviewStatus"] == "accepted"]

if dataset.items_count == 0:
    sly.logger.info(f"Dataset {dataset.name} is empty")
    app.stop()
elif len(reviewed_item_ids) == 0:
    sly.logger.info(f"No reviewed {items_type} found")
    app.stop()
else:
    sly.logger.info(f"Found {len(reviewed_item_ids)} reviewed {items_type}")

# make project directory path
STORAGE_DIR = sly.app.get_data_dir()
project_dir = os.path.join(STORAGE_DIR, f"{job.id}_job_{project.id}_{project.name}")

# get project meta
meta_json = api.project.get_meta(id=project.id)
project_meta = sly.ProjectMeta.from_json(meta_json)


sly.logger.info(f"Project type is {project.type}")
if project.type == str(sly.ProjectType.VIDEOS):
    export_videos(api, dataset, reviewed_item_ids, project_dir, project_meta)
elif project.type == str(sly.ProjectType.IMAGES):
    export_images(api, dataset, reviewed_item_ids, project_dir, project_meta)
elif project.type == str(sly.ProjectType.POINT_CLOUDS):
    export_pointclouds(api, dataset, reviewed_item_ids, project_dir, project_meta)
else:
    raise RuntimeError(f"Project type {project.type} is not supported")

result_archive = f"{project_dir}.tar.gz"
sly.fs.archive_directory(project_dir, result_archive)
sly.output.set_download(result_archive)

reviewed = len(reviewed_item_ids)
not_reviewed = dataset.items_count - reviewed

sly.logger.info(
    f"""
Dataset {dataset.name} has {dataset.items_count} {items_type}:
    * {reviewed} reviewed {items_type} - processed;
    * {not_reviewed} not reviewed {items_type} - skipped.
"""
)

app.stop()
