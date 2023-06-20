if [ ! -d "datasets/inbo-watervogels" ]; then
    echo "Downloading INBO watervogels dataset (https://www.gbif.org/dataset/7f9eb622-c036-44c6-8be9-5793eaa1fa1e)..."
    wget https://ipt.inbo.be/archive.do?r=watervogels-occurrences -O inbo-watervogels.zip
    unzip inbo-watervogels.zip -d datasets/inbo-watervogels
    rm inbo-watervogels.zip
    echo "Downloaded INBO watervogels dataset."
else
    echo "INBO watervogels dataset already downloaded."
fi

if [ ! -d "datasets/gbif-backbone" ]; then
    echo "Downloading GBIF backbone dataset (https://www.gbif.org/dataset/d7dddbf4-2cf0-4f39-9b2a-bb099caae36c)..."
    wget https://hosted-datasets.gbif.org/datasets/backbone/current/backbone.zip -O gbif-backbone.zip
    unzip gbif-backbone.zip -d datasets/gbif-backbone
    rm gbif-backbone.zip
    echo "Downloaded GBIF backbone dataset."
else
    echo "GBIF backbone dataset already downloaded."
fi
