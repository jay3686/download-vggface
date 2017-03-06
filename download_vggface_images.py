"""
Script to download images based on the urls in the vggfaces dataset
http://www.robots.ox.ac.uk/~vgg/data/vgg_face/

Loads classes file for 0 indexed mapping of class id to celebrity name

classes generated using:
```
ls vgg_face_dataset/files/ | sed 's/.txt//g' > classes
```

Also generates a master manifest file with path to image and class id
"""

import aiohttp
import asyncio
import hashlib
import os
import magic

from io import BytesIO
from PIL import Image


class_file_base = 'vgg_face_dataset/files'
images_base = 'images'
class_definition_file = 'classes'
missing_image_file = 'missing'
manifest_file = 'master_manifest'
junk_images_dir = 'junk_images'
junk_images_set = set()


def is_junk_image(image_data):
    return hashlib.md5(image_data).hexdigest() in junk_images_set

def valid_image_mimetype(fobject):
    mime = magic.Magic(mime=True)
    mimetype = mime.from_buffer(fobject.read(1024))
    fobject.seek(0)

    return mimetype and mimetype.startswith('image')

@asyncio.coroutine
def process_image(row, class_id,  class_name, session):
    image_id, url = row.split()[:2]
    image_name = os.path.join(images_base, class_name, '%s.jpg' % image_id)

    if os.path.exists(image_name):
        # print('Skipping %s, already exists.' % image_name)
        return

    try:
        req = yield from session.get(url)
        # print('Checking %s %s' % (image_name, url))
        # raise exception for non 200 response.
        if req.status >= 300:
            req.close()
            raise Exception('HTTP ERR %s' % req.status)

        raw_image_data = yield from req.read()

        # junk images are sent by CDN for not found images.
        if is_junk_image(raw_image_data):
            raise IOError('File is a junk image')

        image_data = BytesIO(raw_image_data)
        if not valid_image_mimetype(image_data):
            # print("Invalid Mime Type")
            raise IOError('File does not have image mimetype')

        # raises exception if image can not be loaded.
        Image.open(image_data)

        # write image to disk
        with open(image_name, 'wb') as handler:
            handler.write(raw_image_data)
        # add to manifest file
        with open(manifest_file, 'a') as manifest:
            manifest.write('%s %s \n' % (image_name, class_id))
        print('Added %s to manifest' % image_name)

    except Exception as e:
        print('Error processing %s (%s) %s' % (
                image_name, url, e))
        # add to missing file
        with open(missing_image_file, 'a') as missing:
            missing.write('%s %s ' % (class_id, class_name))
            missing.write(row)

@asyncio.coroutine
def process_class_file(class_id, name, session):
    # create image dir if not exist
    try:
        os.makedirs(os.path.join(images_base, name))
    except OSError:
        # assume error is because dir already exists
        pass

    class_file_name = os.path.join(class_file_base, '%s.txt' % name)
    with open(class_file_name, 'r') as class_file:
        # print('Downloading images for [%s] %s' % (class_id, name))
        tasks = [process_image(row, class_id, name, session) for row in class_file]
    for task in tasks:
        yield from task


if __name__ == '__main__':

    # get class file
    with open(class_definition_file, 'r') as f:
        classes =  [line.strip() for line in f]

    # delete missing/manifest file if it exists
    for file in [manifest_file, missing_image_file]:
        try:
            os.remove(file)
        except OSError:
            pass
    for filename in os.listdir(junk_images_dir):
        with open(os.path.join(junk_images_dir, filename), 'rb') as junk_image:
            junk_images_set.add(hashlib.md5(junk_image.read()).hexdigest())

    loop = asyncio.get_event_loop()
    with aiohttp.ClientSession() as session:
        tasks = [process_class_file(class_id, class_name, session) 
            for class_id, class_name in enumerate(classes)]

        print('Starting loop')
        loop.run_until_complete(asyncio.wait(tasks))
    loop.close()
    print('Finished')

