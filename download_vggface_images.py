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

import os
import magic
import requests
from PIL import Image
from requests.exceptions import RequestException
from StringIO import StringIO


class_file_base = 'vgg_face_dataset/files'
images_base = 'images'
class_definition_file = 'classes'
missing_image_file = 'missing'
manifest_file = 'master_manifest'


def valid_image_mimetype(fobject):
    mime = magic.Magic(mime=True)
    mimetype = mime.from_buffer(fobject.read(1024))
    fobject.seek(0)

    return mimetype and mimetype.startswith('image')

def process_class_file(class_id, name):
    class_file_name = os.path.join(class_file_base, '%s.txt' % name)
    with open(class_file_name, 'r') as class_file:
        print 'Downloading images for [%s] %s' % (class_id, name)

        for row in class_file:
            image_id, url = row.split()[:2]
            image_name = os.path.join(images_base, name, '%s.jpg' % image_id)

            if os.path.exists(image_name):
                print 'Skipping %s, already exists.' % image_name
            else:
                try:
                    print 'Checking %s %s' % (image_name, url)
                    # raises exception for connection errors.
                    req = requests.get(url, timeout=60)
                    # raise exception for non 200 response.
                    if not req.ok:
                        raise RequestException('HTTPErr %s' % req.status_code)

                    image_data = StringIO(req.content)
                    if not valid_image_mimetype(image_data):
                        print "Invalid Mime Type"
                        raise IOError('File does not have image mimetype')

                    # raises exception if image can not be loaded.
                    Image.open(image_data)

                    # write image to disk
                    with open(image_name, 'wb') as handler:
                        handler.write(req.content)
                    # add to manifest file
                    with open(manifest_file, 'a') as manifest:
                        manifest.write('%s %s \n' % (image_name, class_id))
                    print 'Added %s to manifest' % image_name

                except Exception as e:
                    print 'Error processing %s (%s) %s' % (
                            image_name, url, e.message)
                    # add to missing file
                    with open(missing_image_file, 'a') as missing:
                        missing.write('%s %s ' % (class_id, name))
                        missing.write(row)

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

    for class_id, class_name in enumerate(classes):
        # create image dir if not exist
        try:
            os.makedirs(os.path.join(images_base, class_name))
        except OSError:
            # assume error is because dir already exists
            pass

        process_class_file(class_id, class_name)
