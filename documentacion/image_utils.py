from PIL import Image
import io

def compress_image(image_bytes, quality=75, max_width=1200):
    """
    Compresses an image from bytes, resizing it if it exceeds max_width.
    Returns the compressed image as bytes.
    """
    img = Image.open(io.BytesIO(image_bytes))

    # Reducir si la foto viene de celular (3000-4000px)
    if img.width > max_width:
        ratio = max_width / img.width
        new_h = int(img.height * ratio)
        img = img.resize((max_width, new_h), Image.LANCZOS)

    # Convertir RGBA/P a RGB (capturas de pantalla)
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")

    output = io.BytesIO()
    img.save(output, format="JPEG", quality=quality, optimize=True)
    return output.getvalue()

# Usage Example:
# with open("input.jpg", "rb") as f:
#     compressed = compress_image(f.read())
#     with open("output.jpg", "wb") as out:
#         out.write(compressed)
