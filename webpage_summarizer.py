# summarize.py
from langchain_community.document_loaders import WebBaseLoader
from langchain_ollama import OllamaLLM
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain_core.prompts import ChatPromptTemplate
import os
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
from langchain.schema import Document
load_dotenv()


def load_url_content(url: str) -> list[Document]:
    user_agent = os.getenv("USER_AGENT", "Mozilla/5.0")
    headers = {"User-Agent": user_agent}

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    paragraphs = soup.find_all("p")
    text = "\n".join(p.get_text() for p in paragraphs if p.get_text(strip=True))

    return [Document(page_content=text, metadata={"source": url})]
def summarize_webpage(url: str, model_name: str = "llama3.2:latest") -> str:
    # Custom loader instead of WebBaseLoader
    docs = load_url_content(url)
    #loader = WebBaseLoader(url) #load_url_content(url)
    #docs = loader.load()
    llm = OllamaLLM(model=model_name, temperature=0.25)
    from langchain.prompts import ChatPromptTemplate

    prompt = ChatPromptTemplate.from_messages([
    ("system", 
     "You are a professional news summarizer and rewriter. Your task is to produce a clean, well-structured, and factual summary of a news article using Markdown formatting. Start directly with the Markdown title. Do not include any introductory text or headings of your own, Use only the provided content and rephrase all text to avoid direct copying."),
    
    ("user", """
Summarize the following article using the rules below.

### ✅ Content Rules:
1. **Retain all factual information**: Include every detail, statistic, quote, and section present in the original article.    
2. **Avoid copyright infringement**: Rephrase all text in original language using your own descriptive tone. Do not copy or reuse phrasing from the original content.
3. **Markdown formatting**: Use `#` for the main title (taken exactly from the article), `##` for sections or subheadings, and normal paragraph text for content. Use `**bold**`, `_italic_`, for dates, number, percentage, important terms, etc. Use line breaks (`\n\n`) as needed.
4. **Maintain logical structure**: If the article has sections or subheadings, maintain those clearly in your Markdown output. 
5. **No personal opinion or additions**: Do not include your own conclusion, summary, or any commentary not found in the original article.
6. **Descriptive and human-like tone**: Write in a natural and engaging tone that feels like it was written by a skilled journalist or news editor, while avoiding similarity to the original sentence structure.
7. **Omit disclaimers** — exclude standard legal or opinion disclaimers (e.g., “The views and opinions expressed...” notes).
8. **Skip non-text elements**: Completely ignore images, charts, or graphics. Do **not** include placeholders like [image],[chart], [insert], or similar references.
     
### Do NOT:
- Invent or add headings, subheadings, or summary sections.
- Include conclusions, takeaways, or commentary.
- Add any personal opinions or interpretations.
- Include **any** section or sentence that suggests visit other pages, downloading files — even if such content exists in the source.
- Mention the author, publishing site, content provider, source URLs, or affiliated services — **remove these entirely**.


Input: doc containing the full content of the article, including all sections, paragraphs.

Output: A Markdown-formatted rephrased article, preserving all factual and structural integrity, without any copied sentences.

Begin by extracting the full content and structure, and regenerate the Markdown output accordingly.

{context}
""")
])
    chain = create_stuff_documents_chain(llm, prompt)
    result = chain.invoke({"context": docs})

    return result
import time

if __name__ == "__main__":
    start_time = time.time() 
    
    try:
        user_agent = os.getenv("USER_AGENT")
        url = "https://www.nasdaq.com/articles/validea-detailed-fundamental-analysis-tgt-105"
        summary = summarize_webpage(url)
        print("\n--- Summary ---\n")
        print(summary)
    except Exception as e:
        print(f"Error: {e}")
    
    end_time = time.time() 
    total_time = end_time - start_time
    print(f"\n⏱️ Total time taken: {total_time:.2f} seconds")