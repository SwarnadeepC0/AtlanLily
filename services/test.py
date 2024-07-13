models = {}
models['filesystem'] = {
    'id' : 'filesystem',
    'name' : 'filesystem',
    'classType' : [
        {'id' : 'folder'},
        {'id' : 'files'}
    ],
    'properties' : [
        {
            'id' : 'filesystem.modifiedAt',
            'name' : 'modifiedAt',
            'propagate' : False,
            'classType' : ['folder', 'files']
        }
    ],
    'relationship' : [
        {
            'from' : 'folder',
            'to' : 'file',
            'type' : 'parentChild'
        }
    ]
}
models['resource'] = {
    'id' : 'resource',
    'name' : 'resource',
    'classType' : [
        {'id' : 'resource'}
    ],
    'properties' : [
        {
            'id' : 'core.name',
            'name' : 'name',
            'propagate' : False,
            'classType' : ['folder', 'files', 'resource']
        }
    ],
    'relationship' : [
        {
            'from' : 'resource',
            'to' : 'file',
            'type' : 'parentChild'
        },
        {
            'from' : 'resource',
            'to' : 'folder',
            'type' : 'parentChild'
        }
    ]
}
models['custom'] = {
    'id' : 'custom',
    'name' : 'custom'
}


def get_all_attributes(self, class_type, model_name, custom=False) :
    attrs = []
    for key, value in self.models.items :
        if key == 'custom' and custom == False :
            continue
        else :
            if 'properties' in value and class_type in value['properties']['classType'] :
                attrs.append(class_type)
    return class_type


def get_all_classtype(self, model_name, custom=False) :
    return self.models[model_name]['classType']


def get_child_classtypes(self, class_type) :
    child_classTypes = []
    for key, value in self.models :
        for relationship in value['relationship'] :
            if class_type in relationship['folder'] :
                child_classTypes.append(class_type)
    return child_classTypes


def add_attr(self, user_provided_property) :
    for property in self.models['custom']['properties'] :
        property['name'] = user_provided_property['name']
        property['classType'] = user_provided_property['classType']
        property['propagate'] = user_provided_property['propagate']
