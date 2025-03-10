from .moodle_mod_helper import ModuleHelper


class mod_forum:
    def __init__(self, moodle_rest) -> None:
        self.helper = ModuleHelper(moodle_rest, modtype='forum', component_name='component', content_field='discussion', has_subcomponents=False) # We do have subcomponents, but they need API calls to get them
        self.moodle_rest = moodle_rest

    def get_forum_content(self, course_modules, course):
        forum_modules = course_modules[(course_modules['modname'] == 'forum')]
        forum_modules_content = self.helper.get_mod_content(forum_modules, course)
        forum_all_content = []
        for _, forum in forum_modules_content.iterrows():
            try:
                forum_id = forum['forum_id']
                forum_discussions = self.moodle_rest.get_forum_discussions(forum_id)
                merged_forum_discussions = self.helper.process_forum_discussions(forum_discussions, forum, course)
                for merged_forum_discussion in merged_forum_discussions:
                    if "forum_discussion_id" not in merged_forum_discussion or merged_forum_discussion['forum_discussion_id'] is None:
                        forum_discussion_posts = {'posts': []}
                    else:
                        forum_discussion_id = merged_forum_discussion['forum_discussion_discussion']
                        forum_discussion_posts = self.moodle_rest.get_forum_discussion_posts(forum_discussion_id)

                    if forum_discussion_posts is None or "exception" in forum_discussion_posts:
                        forum_discussion_posts = {'posts': []}
                    merged_forum_discussion_posts = self.helper.process_forum_discussion_posts(forum_discussion_posts, merged_forum_discussion, course)
                    forum_all_content.append(merged_forum_discussion_posts)

            except Exception as e:
                print(f"An get forum posts or discussions error occurred: {e}")
        return forum_all_content


