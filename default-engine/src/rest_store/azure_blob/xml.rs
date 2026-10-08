use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{ObjectMeta, Result};
use serde::{Deserialize, Serialize};

use super::error;

#[derive(Deserialize)]
#[serde(rename = "EnumerationResults")]
struct EnumerationResults {
    #[serde(rename = "Blobs")]
    blobs: Blobs,
    #[serde(rename = "NextMarker", default)]
    marker: String,
}

#[derive(Deserialize)]
struct Blobs {
    #[serde(rename = "Blob", default)]
    objects: Vec<Blob>,
}

#[derive(Deserialize)]
struct Blob {
    #[serde(rename = "Name")]
    name: Name,
    #[serde(rename = "Properties")]
    properties: Properties,
}

#[derive(Deserialize)]
struct Name {
    #[serde(rename = "@Encoded", default)]
    encoded: bool,
    #[serde(rename = "$text", default)]
    value: String,
}

#[derive(Deserialize)]
struct Properties {
    #[serde(rename = "Content-Length")]
    size: u64,
    #[serde(rename = "Last-Modified")]
    modified: String,
    #[serde(rename = "Etag")]
    etag: String,
    #[serde(rename = "ResourceType", default)]
    resource_type: String,
}

pub(super) struct Page {
    pub objects: Vec<ObjectMeta>,
    pub marker: String,
}

pub(super) fn parse_list(body: &[u8]) -> Result<Page> {
    let mut reader = quick_xml::Reader::from_reader(body);
    let mut elements = Vec::<Vec<u8>>::new();
    let mut name_whitespace = Vec::new();
    let mut root_seen = false;
    loop {
        match reader
            .read_event()
            .map_err(|_| error("malformed Blob listing XML"))?
        {
            quick_xml::events::Event::Start(element) => {
                if elements.is_empty() {
                    if root_seen || element.name().as_ref() != b"EnumerationResults" {
                        return Err(error("expected one Blob EnumerationResults XML root"));
                    }
                    root_seen = true;
                }
                if element.name().as_ref() == b"Name"
                    && elements.iter().map(Vec::as_slice).eq([
                        b"EnumerationResults".as_slice(),
                        b"Blobs".as_slice(),
                        b"Blob".as_slice(),
                    ])
                {
                    let text = reader
                        .read_text(element.name())
                        .map_err(|_| error("malformed Blob listing name"))?;
                    name_whitespace.push(
                        (!text.is_empty() && text.trim().is_empty()).then(|| text.into_owned()),
                    );
                } else {
                    elements.push(element.name().as_ref().to_vec());
                }
            }
            quick_xml::events::Event::End(_) => {
                elements
                    .pop()
                    .ok_or_else(|| error("malformed Blob listing XML"))?;
            }
            quick_xml::events::Event::Empty(element) if !elements.is_empty() => {
                if element.name().as_ref() == b"Name"
                    && elements.iter().map(Vec::as_slice).eq([
                        b"EnumerationResults".as_slice(),
                        b"Blobs".as_slice(),
                        b"Blob".as_slice(),
                    ])
                {
                    name_whitespace.push(None);
                }
            }
            quick_xml::events::Event::Eof if root_seen && elements.is_empty() => break,
            quick_xml::events::Event::Decl(_) if !root_seen => {}
            quick_xml::events::Event::Comment(_) | quick_xml::events::Event::PI(_) => {}
            quick_xml::events::Event::Text(text)
                if !elements.is_empty() || text.as_ref().iter().all(u8::is_ascii_whitespace) => {}
            quick_xml::events::Event::CData(_) | quick_xml::events::Event::GeneralRef(_)
                if !elements.is_empty() => {}
            _ => return Err(error("expected Blob EnumerationResults XML root")),
        }
    }
    let page: EnumerationResults =
        quick_xml::de::from_reader(body).map_err(|_| error("malformed Blob listing XML"))?;
    if name_whitespace.len() != page.blobs.objects.len() {
        return Err(error("malformed Blob listing names"));
    }
    let mut objects = Vec::with_capacity(page.blobs.objects.len());
    for (mut blob, whitespace) in page.blobs.objects.into_iter().zip(name_whitespace) {
        if blob.name.value.is_empty() {
            if let Some(whitespace) = whitespace {
                blob.name.value = whitespace;
            }
        }
        if blob.name.encoded {
            return Err(super::unsupported("encoded Blob listing names"));
        }
        if blob.properties.resource_type == "directory" {
            continue;
        }
        let location =
            Path::parse(&blob.name.value).map_err(|_| error("invalid Blob listing path"))?;
        if location.as_ref() != blob.name.value || location.as_ref().is_empty() {
            return Err(error("Blob listing path would be normalized"));
        }
        objects.push(ObjectMeta {
            location,
            size: blob.properties.size,
            last_modified: chrono::DateTime::parse_from_rfc2822(&blob.properties.modified)
                .map_err(|_| error("invalid Blob listing Last-Modified"))?
                .with_timezone(&chrono::Utc),
            e_tag: Some(blob.properties.etag),
            version: None,
        });
    }
    Ok(Page {
        objects,
        marker: page.marker,
    })
}

pub(super) fn block_list(ids: &[String]) -> Result<String> {
    #[derive(Serialize)]
    #[serde(rename = "BlockList")]
    struct BlockList<'a> {
        #[serde(rename = "Latest")]
        ids: &'a [String],
    }
    quick_xml::se::to_string(&BlockList { ids })
        .map_err(|_| error("cannot serialize Blob block list"))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(" ")]
    #[case("  ")]
    #[case(" a ")]
    fn azure_blob_listing_preserves_whitespace_names(#[case] name: &str) {
        let body = format!("<EnumerationResults><Blobs><Blob><Name>{name}</Name><Properties><Content-Length>0</Content-Length><Last-Modified>Wed, 07 Oct 2026 10:00:00 GMT</Last-Modified><Etag>etag</Etag></Properties></Blob></Blobs><NextMarker /></EnumerationResults>");
        let page = parse_list(body.as_bytes()).unwrap();
        assert_eq!(page.objects[0].location.as_ref(), name);
    }
}
