import asyncio
import os
import time
from datetime import datetime

import nest_asyncio
from firecrawl import FirecrawlApp
from ragflow_sdk import RAGFlow

MD_CACHE_PATH = "./data/markdown_cache"
WATERMARK_PATH = "./data/watermarks"

FIRECRAWL_API_URL = "http://192.168.2.17:3002"
FIRECRAWL_API_KEY = ""
RAGFLOW_API_URL = "http://192.168.2.17"
RAGFLOW_API_KEY = "ragflow-E1ODI1OGRjZjdiMzExZWY5M2YzMDI0Mm"

app_firecrawl = FirecrawlApp(api_key=FIRECRAWL_API_KEY, api_url=FIRECRAWL_API_URL)

app_ragflow = RAGFlow(api_key=RAGFLOW_API_KEY, base_url=RAGFLOW_API_URL, version="v1")

os.makedirs(MD_CACHE_PATH, exist_ok=True)
os.makedirs(WATERMARK_PATH, exist_ok=True)

cur_datetime = datetime.fromtimestamp(time.time()).strftime("%Y%m%d_%H%M%S")
watermark_file = f".wm_md"

crawled_docs = []

uri = "docs.firecrawl.dev"
protocol = "https"
# scrape_status = app_firecrawl.scrape_url(
#     f"{protocol}://{uri}", params={"formats": ["markdown", "html"]}
# )
# print(
#     f"keys of scrape status: ",
#     scrape_status.keys(),
#     "\nmeta data: ",
#     scrape_status["metadata"],
# )
# md_content = scrape_status["markdown"]
# crawled_docs.append({"name": f"{uri}.md", "content": md_content})

# # write to file
# md_file = f"{uri}.md"
# md_path = os.path.join(MD_CACHE_PATH, md_file)
# with open(md_path, "w", encoding="utf-8") as f:
#     f.write(md_content)
#
# # get the site map
# map_result = app_firecrawl.map_url(f"{protocol}://{uri}")
# print(map_result)


# async crawl with websocket
async def async_crawl():

    nest_asyncio.apply()

    # Define event handlers
    def on_document(detail):
        print(f"DOC received, {detail["id"]=} {detail["data"]["metadata"]["url"]=}")
        crawled_docs.append(
            {
                "name": f"{detail["data"]["metadata"]["url"].split("//")[1]}.md",
                "content": detail["data"]["markdown"],
            }
        )

    def on_error(detail):
        print(f"ERR, {detail["error"]=}")

    def on_done(detail):
        print(f"DONE, {detail["status"]=}")

    # Function to start the crawl and watch process
    async def start_crawl_and_watch():
        # Initiate the crawl job and get the watcher
        watcher = app_firecrawl.crawl_url_and_watch(
            f"{protocol}://{uri}", {"excludePaths": ["blog/*"], "limit": 5}
        )

        # Add event listeners
        watcher.add_event_listener("document", on_document)
        watcher.add_event_listener("error", on_error)
        watcher.add_event_listener("done", on_done)

        # Start the watcher
        await watcher.connect()

    # Run the event loop
    await start_crawl_and_watch()


asyncio.run(async_crawl())


rag_dataset_name = "edp-knowledge-base"
rag_dataset_description = "knowledge base of edp project"
try:
    app_ragflow.create_dataset(
        name=rag_dataset_name,
        description=rag_dataset_description,
        embedding_model="BAAI/bge-base-en-v1.5",
        permission="me",
        chunk_method="naive",
    )
except Exception as e:
    print(f"app_ragflow.create_dataset {e=}")


rag_dataset_list = app_ragflow.list_datasets(
    page=0, page_size=100, orderby="create_time", desc=True, name=rag_dataset_name
)
rag_dataset = rag_dataset_list[0]

document_list = []
for crawled_doc in crawled_docs:
    try:
        matched_docs = rag_dataset.list_documents(
            page=1,
            page_size=100,
            orderby="create_time",
            desc=True,
            keywords=crawled_doc["name"],
        )
        doc_id = matched_docs[0].id
        rag_dataset.delete_documents(ids=[doc_id])
    except Exception as e:
        print(f"rag_dataset.list_documents {e=}")

    document_list.append(
        {"display_name": crawled_doc["name"], "blob": crawled_doc["content"]}
    )

documents = rag_dataset.upload_documents(document_list)
doc_ids = []
for doc in documents:
    doc_ids.append(doc.id)
rag_dataset.async_parse_documents(doc_ids)
