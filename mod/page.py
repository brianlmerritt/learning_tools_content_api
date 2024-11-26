from .moodle_mod_helper import ModuleHelper


class mod_page:
    def __init__(self, moodle_rest) -> None:
        self.helper = ModuleHelper(moodle_rest, modtype='page', component_name='component', content_field='contents', has_subcomponents=True)
        
    def get_page_content(self, course_modules, course):
        page_modules = course_modules[(course_modules['modname'] == 'page')]
        return self.helper.get_mod_content(page_modules, course)