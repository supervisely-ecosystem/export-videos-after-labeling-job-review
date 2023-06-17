import os
import supervisely as sly

from dotenv import load_dotenv
from tqdm import tqdm
from supervisely.video_annotation.key_id_map import KeyIdMap

if sly.is_development():
    load_dotenv("local.env")
    load_dotenv(os.path.expanduser("~/supervisely.env"))

api = sly.Api.from_env()

JOB_ID = os.environ.get("modal.state.slyJobId")

app = sly.Application()


if JOB_ID is None:
    raise RuntimeError("Job id is not specified")

job = api.labeling_job.get_info_by_id(JOB_ID)
project = api.project.get_info_by_id(job.project_id)
dataset = api.dataset.get_info_by_id(job.dataset_id)

if job is None or dataset is None or project is None:
    raise RuntimeError("Job, project or dataset not found")

reviewed_video_ids = [video["id"] for video in job.entities if video["reviewStatus"] == "accepted"]
sly.logger.info(f"Found {len(reviewed_video_ids)} reviewed videos")

# make project directory path
STORAGE_DIR = sly.app.get_data_dir()
project_dir = os.path.join(STORAGE_DIR, f"{project.id}_{project.name}")

# get project meta
meta_json = api.project.get_meta(id=project.id)
project_meta = sly.ProjectMeta.from_json(meta_json)

# create local project and dataset
key_id_map = KeyIdMap()
project_fs = sly.VideoProject(project_dir, sly.OpenMode.CREATE)
project_fs.set_meta(project_meta)
dataset_fs = project_fs.create_dataset(dataset.name)

# get reviewed videos infos
videos = api.video.get_list(
    dataset.id,
    filters=[{"field": "id", "operator": "in", "value": reviewed_video_ids}],
)


progress = tqdm(total=len(videos), desc=f"Downloading videos...")
for batch in sly.batched(videos, batch_size=10):
    video_ids = [video_info.id for video_info in batch]
    video_names = [video_info.name for video_info in batch]
    ann_jsons = api.video.annotation.download_bulk(dataset.id, video_ids)
    for video_id, video_name, ann_json in zip(video_ids, video_names, ann_jsons):
        video_ann = sly.VideoAnnotation.from_json(ann_json, project_meta, key_id_map)

        video_file_path = dataset_fs.generate_item_path(video_name)
        api.video.download_path(video_id, video_file_path)
        dataset_fs.add_item_file(video_name, video_file_path, ann=video_ann, _validate_item=False)

    progress.update(len(batch))

project_fs.set_key_id_map(key_id_map)
sly.output.set_download(project_dir)


reviewed = len(reviewed_video_ids)
not_reviewed = dataset.items_count - reviewed
sly.logger.info(
    f"""
Dataset {dataset.name} has {dataset.items_count} videos:
    * {reviewed} reviewed videos - processed;
    * {not_reviewed} not reviewed videos - skipped.
"""
)

app.stop()
