from .moodle_mod_helper import ModuleHelper


class mod_resource:
    def __init__(self, moodle_rest) -> None:
        self.resource_helper = ModuleHelper(moodle_rest, modtype='resource', component_name='file', content_field='contents', has_subcomponents=True)
        self.folder_helper = ModuleHelper(moodle_rest, modtype='folder', component_name='file', content_field='contents', has_subcomponents=True)
        
    

    def get_resource_content(self, course_resources, course):
        resource_modules = course_resources[(course_resources['modname'] == 'resource')]
        resource_list = self.resource_helper.get_mod_content(resource_modules, course)
        folder_modules = course_resources[(course_resources['modname'] == 'folder')]
        folder_list = self.folder_helper.get_mod_content(folder_modules, course)
        return resource_list, folder_list