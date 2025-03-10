from .moodle_mod_helper import ModuleHelper


class mod_url:
    def __init__(self, moodle_rest) -> None:
        self.helper = ModuleHelper(moodle_rest, modtype='url', component_name='component', content_field='url', has_subcomponents=False)
        
    def get_url_content(self, course_modules, course):
        url_modules = course_modules[(course_modules['modname'] == 'url')]
        return self.helper.get_mod_content(url_modules, course)